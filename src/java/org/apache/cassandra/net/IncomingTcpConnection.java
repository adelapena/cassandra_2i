/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.UnknownColumnFamilyException;
import org.apache.cassandra.gms.Gossiper;

public class IncomingTcpConnection extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private final int version;
    private final boolean compressed;
    private final Socket socket;
    public InetAddress from;

    public IncomingTcpConnection(int version, boolean compressed, Socket socket)
    {
        assert socket != null;
        this.version = version;
        this.compressed = compressed;
        this.socket = socket;
        if (DatabaseDescriptor.getInternodeRecvBufferSize() != null)
        {
            try
            {
                this.socket.setReceiveBufferSize(DatabaseDescriptor.getInternodeRecvBufferSize());
            }
            catch (SocketException se)
            {
                logger.warn("Failed to set receive buffer size on internode socket.", se);
            }
        }
    }

    /**
     * A new connection will either stream or message for its entire lifetime: because streaming
     * bypasses the InputStream implementations to use sendFile, we cannot begin buffering until
     * we've determined the type of the connection.
     */
    @Override
    public void run()
    {
        try
        {
            if (version < MessagingService.VERSION_12)
                handleLegacyVersion();
            else
                handleModernVersion();
        }
        catch (EOFException e)
        {
            logger.trace("eof reading from socket; closing", e);
            // connection will be reset so no need to throw an exception.
        }
        catch (UnknownColumnFamilyException e)
        {
            logger.warn("UnknownColumnFamilyException reading from socket; closing", e);
        }
        catch (IOException e)
        {
            logger.debug("IOException reading from socket; closing", e);
        }
        finally
        {
            close();
        }
    }

    private void handleModernVersion() throws IOException
    {
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        out.writeInt(MessagingService.current_version);
        out.flush();

        DataInputStream in = new DataInputStream(socket.getInputStream());
        int maxVersion = in.readInt();
        from = CompactEndpointSerializationHelper.deserialize(in);

        if (compressed)
        {
            logger.debug("Upgrading incoming connection to be compressed");
            in = new DataInputStream(new SnappyInputStream(socket.getInputStream()));
        }
        else
        {
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 4096));
        }

        logger.debug("Max version for {} is {}", from, maxVersion);
        if (version > MessagingService.current_version)
        {
            // save the endpoint so gossip will reconnect to it
            Gossiper.instance.addSavedEndpoint(from);
            logger.info("Received messages from newer protocol version {}. Ignoring", version);
            return;
        }
        MessagingService.instance().setVersion(from, Math.min(MessagingService.current_version, maxVersion));
        logger.debug("set version for {} to {}", from, Math.min(MessagingService.current_version, maxVersion));
        // outbound side will reconnect if necessary to upgrade version

        while (true)
        {
            MessagingService.validateMagic(in.readInt());
            receiveMessage(in, version);
        }
    }

    private void handleLegacyVersion()
    {
        throw new UnsupportedOperationException("Unable to read obsolete message version " + version + "; the earliest version supported is 1.2.0");
    }

    private InetAddress receiveMessage(DataInputStream input, int version) throws IOException
    {
        int id;
        if (version < MessagingService.VERSION_20)
            id = Integer.parseInt(input.readUTF());
        else
            id = input.readInt();

        long timestamp = System.currentTimeMillis();
        // make sure to readInt, even if cross_node_to is not enabled
        int partial = input.readInt();
        if (DatabaseDescriptor.hasCrossNodeTimeout())
            timestamp = (timestamp & 0xFFFFFFFF00000000L) | (((partial & 0xFFFFFFFFL) << 2) >> 2);

        MessageIn message = MessageIn.read(input, version, id);
        if (message == null)
        {
            // callback expired; nothing to do
            return null;
        }
        if (version <= MessagingService.current_version)
        {
            MessagingService.instance().receive(message, id, timestamp);
        }
        else
        {
            logger.debug("Received connection from newer protocol version {}. Ignoring message", version);
        }
        return message.from;
    }

    private void close()
    {
        // reset version here, since we set when starting an incoming socket
        if (from != null)
            MessagingService.instance().resetVersion(from);
        try
        {
            socket.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("error closing socket", e);
        }
    }
}
