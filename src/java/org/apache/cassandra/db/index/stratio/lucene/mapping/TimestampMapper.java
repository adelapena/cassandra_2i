package org.apache.cassandra.db.index.stratio.lucene.mapping;

import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;

public class TimestampMapper {
	
	/** The Lucene's field name. */
	private static final String FIELD_NAME = "timestamp";

	/**
	 * Returns the timestamp of the specified {@link DecoratedColumn} as a not indexed, stored
	 * Lucene's {@link Field}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the timestamp of the specified {@link DecoratedColumn} as a not indexed, stored
	 *         Lucene's {@link Field}.
	 */
	public Field field(DecoratedColumn decoratedColumn) {
		Long timestamp = decoratedColumn.getTimestamp();
		return new LongField(FIELD_NAME, timestamp, Store.NO);
	}
	
	/**
	 * 
	 * @param value
	 * @return
	 */
	public Query query(Long value) {
		return NumericRangeQuery.newLongRange(FIELD_NAME, value, value, true, true);
	}
	
	/**
	 * 
	 * @param min
	 * @param max
	 * @param minInclusive
	 * @param maxInclusive
	 * @return
	 */
	public Query query(Long min, Long max, final boolean minInclusive, final boolean maxInclusive) {
		return NumericRangeQuery.newLongRange(FIELD_NAME, min, max, minInclusive, maxInclusive);
	}

	/**
	 * Returns the timestamp contained in the specified Lucene's {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document}.
	 * @return the timestamp contained in the specified Lucene's {@link Document}.
	 */
	public Long value(Document document) {
		return document.getField(FIELD_NAME).numericValue().longValue();
	}

}
