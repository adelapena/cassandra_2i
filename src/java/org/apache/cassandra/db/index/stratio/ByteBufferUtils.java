package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

public class ByteBufferUtils {

	public static byte[] asArray(ByteBuffer key) {
		ByteBuffer bb = ByteBufferUtil.clone(key);
		byte[] bytes = new byte[bb.remaining()];
		bb.get(bytes);
		return bytes;
	}

	public static ByteBuffer[] split(ByteBuffer bb, AbstractType<?> comparator) {
		if (comparator instanceof CompositeType) {
			return ((CompositeType) comparator).split(bb);
		} else {
			return new ByteBuffer[] { bb };
		}
	}

	public static String toString(ByteBuffer bb, AbstractType<?> comparator) {
		if (comparator instanceof CompositeType) {
			CompositeType composite = (CompositeType) comparator;
			List<AbstractType<?>> types = composite.types;
			ByteBuffer[] components = composite.split(bb);
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < types.size(); i++) {
				AbstractType<?> type = types.get(i);
				ByteBuffer component = components[i];
				sb.append(type.compose(component));
				if (i < types.size() - 1) {
					sb.append(':');
				}
			}
			return sb.toString();
		} else {
			return comparator.compose(bb).toString();
		}
	}

	public static String toHex(ByteBuffer bb) {
		byte[] bytes = asArray(bb);
		return Hex.bytesToHex(bytes);
	}

	public static ByteBuffer fromHex(String string) {
		byte[] bytes = Hex.hexToBytes(string);
		;
		return ByteBuffer.wrap(bytes);
	}

}