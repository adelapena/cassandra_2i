package org.apache.cassandra.db.index.stratio;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractCompositeType.CompositeComponent;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Class which decorates a CQL3 {@link Column} with some contextual info taken from the
 * partition/row key and the schema definition.
 * 
 * @author adelapena
 * 
 */
public class DecoratedColumn {

	private final ByteBuffer partitionKey;
	private final Column column;
	private final ColumnDefinition columnDefinition;
	private AbstractType<?> keyType;
	private CompositeType nameType;
	private AbstractType<?> valueType;

	/**
	 * Builds a new {@link DecoratedColumn} decorating the specified {@link Column} with the
	 * specified info.
	 * 
	 * @param partitionKey
	 *            the partition key, other words the storage engine row key
	 * @param column
	 *            the {@link Column} to be decorated.
	 * @param baseCfs
	 *            the {@link ColumnFamilyStore} of the {@link Column} to be decorated.
	 * @param columnDefinition
	 *            the {@link ColumnDefinition} of the {@link Column} to be decorated.
	 */
	public DecoratedColumn(ByteBuffer partitionKey,
	                       Column column,
	                       ColumnFamilyStore baseCfs,
	                       ColumnDefinition columnDefinition) {
		this.partitionKey = partitionKey;
		this.column = column;
		this.columnDefinition = columnDefinition;
		keyType = baseCfs.metadata.getKeyValidator();
		nameType = (CompositeType) baseCfs.getComparator();
		valueType = columnDefinition.getValidator();
	}

	/**
	 * Returns the decorated {@link Column}.
	 * 
	 * @return the decorated {@link Column}.
	 */
	public Column getColumn() {
		return column;
	}

	/**
	 * Returns the {@link ColumnDefinition} of the decorated {@link Column}.
	 * 
	 * @return the {@link ColumnDefinition} of the decorated {@link Column}.
	 */
	public ColumnDefinition getColumnDefinition() {
		return columnDefinition;
	}

	/**
	 * Returns the {@link ColumnDefinition.Type} of the decorated {@link Column}.
	 * 
	 * @return the {@link ColumnDefinition.Type} of the decorated {@link Column}.
	 */
	public ColumnDefinition.Type getType() {
		return columnDefinition.type;
	}

	/**
	 * Returns the type of the key of the decorated {@link Column}.
	 * 
	 * @return the type of the key of the decorated {@link Column}.
	 */
	public AbstractType<?> getKeyType() {
		return keyType;
	}

	/**
	 * Returns the type of the name of the decorated {@link Column} as a {@link CompositeType}.
	 * 
	 * @return the type of the name of the decorated {@link Column} as a {@link CompositeType}.
	 */
	public CompositeType getNameType() {
		return nameType;
	}

	/**
	 * Returns the type of the value of the decorated {@link Column} as a {@code ByteBuffer}.
	 * 
	 * @return the type of the value of the decorated {@link Column} as a {@code ByteBuffer}.
	 */
	public AbstractType<?> getValueType() {
		return valueType;
	}

	/**
	 * Returns the partition key (other words, the key of the storage engine row) of the decorated
	 * {@link Column} as a {@code ByteBuffer}.
	 * 
	 * @return the partition key of the decorated {@link Column} as a {@code ByteBuffer}.
	 */
	public ByteBuffer getPartitionKey() {
		return partitionKey;
	}

	/**
	 * Returns the partition key (other words, the key of the storage engine row) of the decorated
	 * {@link Column} as a {@code String}.
	 * 
	 * @return the partition key of the decorated {@link Column} as a {@code String}.
	 */
	public String getPartitionKeyAsString() {
		return ByteBufferUtils.toString(partitionKey, keyType);
	}

	/**
	 * Return the clustering key of the decorated {@link Column}. This key is obtained from the column name.
	 * 
	 * @return the clustering key of the decorated {@link Column}.
	 */
	public ByteBuffer getClusteringKey() {
		ByteBuffer columnName = column.name();
		List<CompositeComponent> components = nameType.deconstruct(columnName);
		components.remove(components.size() - 1);
		CompositeType.Builder builder = nameType.builder();
		for (CompositeComponent cc : components) {
			builder.add(cc.value);
		}
		builder.add(ByteBufferUtil.EMPTY_BYTE_BUFFER);
		return builder.build();
	}

	/**
	 * Returns the full name (as it's view by the storage line) of the decorated {@link Column} as a
	 * {@code ByteBuffer}.
	 * 
	 * @return the full name (as it's view by the storage line) of the decorated {@link Column} as a
	 *         {@code ByteBuffer}.
	 */
	public ByteBuffer getName() {
		return column.name();
	}

	/**
	 * Returns the full name (as it's view by the storage line) of the decorated {@link Column} as a
	 * {@code String}.
	 * 
	 * @return the full name (as it's view by the storage line) of the decorated {@link Column} as a
	 *         {@code String}.
	 */
	public String getNameAsString() {
		return ByteBufferUtils.toString(column.name(), nameType);
	}

	/**
	 * Returns the value of the decorated {@link Column} as a {@code ByteBuffer}.
	 * 
	 * @return the value of the decorated {@link Column} as a {@code ByteBuffer}.
	 */
	public ByteBuffer getValue() {
		switch (columnDefinition.type) {
		case PARTITION_KEY: {
			ByteBuffer[] components = ByteBufferUtils.split(partitionKey, keyType);
			return components[columnDefinition.componentIndex];
		}
		case CLUSTERING_KEY: {
			ByteBuffer[] components = ByteBufferUtils.split(column.name(), nameType);
			return components[columnDefinition.componentIndex];
		}
		default:
			return column.value();
		}
	}

	/**
	 * Returns the unmarshalled value of the decorated {@link Column} as an {@code Object}.
	 * 
	 * @return the unmarshalled value of the decorated {@link Column} as an {@code Object}.
	 */
	public Object getValueAsObject() {
		return valueType.compose(getValue());
	}

	/**
	 * Returns the timestamp of the decorated {@link Column} as an UNIX timestamp.
	 * 
	 * @return the timestamp of the decorated {@link Column} as an UNIX timestamp.
	 */
	public Long getTimestamp() {
		return column.timestamp();
	}

	/**
	 * Returns the {@link Token} of the decorated {@link Column}.
	 * 
	 * @return the {@link Token} of the decorated {@link Column}.
	 */
	public Token<?> getToken() {
		IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
		return partitioner.getToken(partitionKey);
	}
	
	/**
	 * Returns the {@link Token} of the decorated {@link Column} as a {@link ByteBuffer}.
	 * 
	 * @return the {@link Token} of the decorated {@link Column} as a {@link ByteBuffer}.
	 */
	@SuppressWarnings("unchecked")
	public ByteBuffer getTokenAsByteBuffer() {
		IPartitioner<?> partitioner = DatabaseDescriptor.getPartitioner();
		Token<?> token = partitioner.getToken(partitionKey);
		return partitioner.getTokenFactory().toByteArray(token);
	}

	/**
	 * Returns a {@code String} that identifies the decorated {@link Column}. The returned
	 * identifier is composed by the partition key, the clustering key and the timestamp.
	 * 
	 * @return a {@code String} that identifies the decorated {@link Column}.
	 */
	public String getIndentifyingString() {
		StringBuilder sb = new StringBuilder();
		sb.append(getPartitionKeyAsString());
		sb.append(':');
		sb.append(getNameAsString());
		sb.append(':');
		sb.append(getTimestamp());
		return sb.toString();
	}

	@Override
    public String toString() {
	    StringBuilder builder = new StringBuilder();
	    builder.append("DecoratedColumn [partitionKey=");
	    builder.append(getPartitionKeyAsString());
	    builder.append(", name=");
	    builder.append(getNameAsString());
	    builder.append(", value=");
	    builder.append(getValueAsObject());
	    builder.append(", timestamp=");
	    builder.append(getTimestamp());
	    builder.append("]");
	    return builder.toString();
    }

}
