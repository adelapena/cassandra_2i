package org.apache.cassandra.db.index.stratio.lucene.mapping;

import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.FieldInfo.IndexOptions;

public class FullKeyMapper {

	/** The Lucene's field name. */
	public static final String FIELD_NAME = "full_key";

	/** The Lucene's field type for the column's identifier. */
	private static final FieldType FIELD_TYPE = new FieldType();
	static {
		FIELD_TYPE.setIndexed(true);
		FIELD_TYPE.setOmitNorms(true);
		FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
		FIELD_TYPE.setStored(false);
		FIELD_TYPE.setTokenized(false);
		FIELD_TYPE.freeze();
	}

	/**
	 * Returns the unique identifying key of the specified {@link DecoratedColumn} as a indexed, not
	 * stored Lucene's {@link Field}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the unique identifying key of the specified {@link DecoratedColumn} as a indexed, not
	 *         stored Lucene's {@link Field}.
	 */
	public Field field(DecoratedColumn decoratedColumn) {
		String value = decoratedColumn.getIndentifyingString();
		return new Field(FIELD_NAME, value, FIELD_TYPE);
	}

	/**
	 * Returns the clustering key of the specified {@link DecoratedColumn} as a Lucene's
	 * {@link Term}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the clustering key of the specified {@link DecoratedColumn} as a Lucene's
	 *         {@link Term}.
	 */
	public Term term(DecoratedColumn decoratedColumn) {
		String value = decoratedColumn.getIndentifyingString();
		return new Term(FIELD_NAME, value);
	}

}
