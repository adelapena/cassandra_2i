package org.apache.cassandra.db.index.stratio.lucene.mapping;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.index.stratio.ByteBufferUtils;
import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.SortField;

public class TokenMapper {
	
	/** The Lucene's field name. */
	private static final String FIELD_NAME = "token";

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
	 * Returns the token of the specified {@link DecoratedColumn} as an indexed, not stored Lucene's
	 * {@link Field}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the token of the specified {@link DecoratedColumn} as an indexed, not stored Lucene's
	 *         {@link Field}.
	 */
	public Field field(DecoratedColumn decoratedColumn) {
		ByteBuffer token = decoratedColumn.getTokenAsByteBuffer();
		return new Field(FIELD_NAME, ByteBufferUtils.toHex(token), FIELD_TYPE);
	}
	
	public SortField sort() {
		return new SortField(FIELD_NAME, SortField.Type.STRING_VAL);
	}
	
}
