package org.apache.cassandra.db.index.stratio.lucene.mapping;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.cassandra.db.index.stratio.ByteBufferUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.SortField;

public class ClusteringKeyMapper {
	
	/** The Lucene's field name. */
	private static final String FIELD_NAME = "clustering_key";

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
	 * Returns the clustering key of the specified {@link DecoratedColumn} as a not indexed, stored
	 * Lucene's {@link Field}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the clustering key of the specified {@link DecoratedColumn} as a not indexed, stored
	 *         Lucene's {@link Field}.
	 */
	public Field field(DecoratedColumn decoratedColumn) {
		ByteBuffer clusteringKey = decoratedColumn.getClusteringKey();
		return new Field(FIELD_NAME, ByteBufferUtils.toHex(clusteringKey), FIELD_TYPE);
	}

	/**
	 * Returns the clustering key contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document}.
	 * @return the clustering key contained in the specified Lucene's {@link Document}.
	 */
	public ByteBuffer bytes(Document document) {
		String value = document.get(FIELD_NAME);
		return ByteBufferUtils.fromHex(value);
	}
	
	public SortField sort() {
		return new SortField(FIELD_NAME, SortField.Type.STRING_VAL);
	}

}
