package org.apache.cassandra.db.index.stratio.lucene.mapping;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.UUID;

import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

public class ValueMapper {

	/** The Lucene's field name. */
	public static final String FIELD_NAME = "value";

	private final AbstractType<?> type;
	private final QueryParser queryParser;

	public ValueMapper(AbstractType<?> type) {
		this.type = type;
		queryParser = new QueryParser(Version.LUCENE_46, FIELD_NAME, new EnglishAnalyzer(Version.LUCENE_46));
		queryParser.setAllowLeadingWildcard(true);
	}

	/**
	 * Returns the value of the specified {@link DecoratedColumn} as an indexed, not stored Lucene's
	 * {@link Field}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn}.
	 * @return the value of the specified {@link DecoratedColumn} as an indexed, not stored Lucene's
	 *         {@link Field}.
	 */
	public Field field(DecoratedColumn decoratedColumn) {
		Object value = type.compose(decoratedColumn.getValue());
		if (type instanceof AsciiType) {
			return new TextField(FIELD_NAME, (String) value, Store.NO);
		} else if (type instanceof LongType) {
			return new LongField(FIELD_NAME, (Long) value, Store.NO);
		} else if (type instanceof BytesType) {
			throw new UnsupportedOperationException();
		} else if (type instanceof BooleanType) {
			return new IntField(FIELD_NAME, (Boolean) value ? 1 : 0, Store.NO);
		} else if (type instanceof CounterColumnType) {
			return new LongField(FIELD_NAME, (Long) value, Store.NO);
		} else if (type instanceof DecimalType) {
			throw new UnsupportedOperationException(); // TODO: Implement
		} else if (type instanceof DoubleType) {
			return new DoubleField(FIELD_NAME, (Double) value, Store.NO);
		} else if (type instanceof FloatType) {
			return new FloatField(FIELD_NAME, (Float) value, Store.NO);
		} else if (type instanceof InetAddressType) {
			return new StringField(FIELD_NAME, (String) value, Store.NO);
		} else if (type instanceof Int32Type) {
			return new IntField(FIELD_NAME, (Integer) value, Store.NO);
		} else if (type instanceof UTF8Type) {
			return new TextField(FIELD_NAME, (String) value, Store.NO);
		} else if (type instanceof TimestampType) {
			return new LongField(FIELD_NAME, ((Date) value).getTime(), Store.NO);
		} else if (type instanceof UUIDType) {
			return new StringField(FIELD_NAME, ((UUID) value).toString(), Store.NO);
		} else if (type instanceof IntegerType) {
			return new IntField(FIELD_NAME, (Integer) value, Store.NO);
		} else if (type instanceof TimeUUIDType) {
			return new StringField(FIELD_NAME, ((UUID) value).toString(), Store.NO);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Returns a Lucene's {@link Query} for retrieving the {@link Document}s matching the specified
	 * column value.
	 * 
	 * @param columnValue
	 *            the column value to be matched.
	 * @return a Lucene's {@link Query} for retrieving the {@link Document}s matching the specified
	 *         column value.
	 */
	public Query query(ByteBuffer columnValue) {
		Object value = type.compose(columnValue);
		if (type instanceof AsciiType) {
			return query(FIELD_NAME, (String) value);
		} else if (type instanceof LongType) {
			Long numValue = (Long) value;
			return NumericRangeQuery.newLongRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof BytesType) {
			throw new UnsupportedOperationException();
		} else if (type instanceof BooleanType) {
			Integer numValue = (Boolean) value ? 1 : 0;
			return NumericRangeQuery.newIntRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof CounterColumnType) {
			Long numValue = (Long) value;
			return NumericRangeQuery.newLongRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof DecimalType) {
			throw new UnsupportedOperationException(); // TODO: Implement
		} else if (type instanceof DoubleType) {
			Double numValue = (Double) value;
			return NumericRangeQuery.newDoubleRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof FloatType) {
			Float numValue = (Float) value;
			return NumericRangeQuery.newFloatRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof InetAddressType) {
			return new TermQuery(new Term(FIELD_NAME, (String) value)); // TODO: Review
		} else if (type instanceof Int32Type) {
			Integer numValue = (Integer) value;
			return NumericRangeQuery.newIntRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof UTF8Type) {
			return query(FIELD_NAME, (String) value);
		} else if (type instanceof TimestampType) {
			Long numValue = (Long) value;
			return NumericRangeQuery.newLongRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof UUIDType) {
			return new TermQuery(new Term(FIELD_NAME, ((UUID) value).toString()));
		} else if (type instanceof IntegerType) {
			Integer numValue = (Integer) value;
			return NumericRangeQuery.newIntRange(FIELD_NAME, numValue, numValue, true, true);
		} else if (type instanceof TimeUUIDType) {
			return new TermQuery(new Term(FIELD_NAME, ((UUID) value).toString()));
		} else {
			return new TermQuery(new Term(FIELD_NAME, value.toString()));
		}
	}

	/**
	 * Returns a Lucene's {@link Query} for retrieving the {@link Document}s matching the specified
	 * field name and value.
	 * 
	 * @param fieldName
	 *            the Lucene's field name.
	 * @param fieldValue
	 *            the Lucene's field name.
	 * @return a Lucene's {@link Query} for retrieving the {@link Document}s matching the specified
	 *         field name and value.
	 */
	private Query query(String fieldName, String fieldValue) {
		StringBuilder sb = new StringBuilder();
		sb.append(fieldName);
		sb.append(':');
		sb.append(fieldValue);
		try {
			return queryParser.parse(sb.toString());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
}
