package org.apache.cassandra.db.index.stratio.lucene.mapping;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.cassandra.db.index.stratio.ByteBufferUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;

public class PartitionKeyMapper {
	
	/** The Lucene's field name. */
	private static final String FIELD_NAME = "partition_key";

	/** The Lucene's field type. */
	private static final FieldType FIELD_TYPE = new FieldType();
	static {
		FIELD_TYPE.setIndexed(true);
		FIELD_TYPE.setOmitNorms(true);
		FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
		FIELD_TYPE.setStored(true);
		FIELD_TYPE.setTokenized(false);
		FIELD_TYPE.freeze();
	}

	/**
	 * Returns the partition key of the specified {@link DecoratedColumn} as a not indexed, stored
	 * Lucene's {@link Field}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the partition key of the specified {@link DecoratedColumn} as a not indexed, stored
	 *         Lucene's {@link Field}.
	 */
	public Field field(DecoratedColumn decoratedColumn) {
		ByteBuffer partitionKey = decoratedColumn.getPartitionKey();
		return new Field(FIELD_NAME, ByteBufferUtils.toHex(partitionKey), FIELD_TYPE);
	}

	/**
	 * Returns the partition key contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document}.
	 * @return the partition key contained in the specified Lucene's {@link Document}.
	 */
	public ByteBuffer bytes(Document document) {
		String value = document.get(FIELD_NAME);
		return ByteBufferUtils.fromHex(value);
	}

}
