package org.apache.cassandra.db.index.stratio.lucene.mapping;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

/**
 * Class which offers functions to convert columns between Cassandra and Lucene data models.
 * 
 * @author adelapena
 * 
 */
public class ColumnMapper {

	public static final String TIMESTAMP_FIELD_NAME = "timestamp";
	public static final String FULL_KEY_FIELD_NAME = "full_key";

	private final FullKeyMapper fullKeyMapper;
	private final TokenMapper tokenMapper;
	private final PartitionKeyMapper partitionKeyMapper;
	private final ClusteringKeyMapper clusteringKeyMapper;
	private final ValueMapper valueMapper;
	private final TimestampMapper timestampMapper;

	public ColumnMapper(ColumnDefinition columnDefinition) {
		AbstractType<?> type = columnDefinition.getValidator();
		fullKeyMapper = new FullKeyMapper();
		tokenMapper = new TokenMapper();
		partitionKeyMapper = new PartitionKeyMapper();
		clusteringKeyMapper = new ClusteringKeyMapper();
		valueMapper = new ValueMapper(type);
		timestampMapper = new TimestampMapper();
	}

	/**
	 * Returns the specified {@link DecoratedColumn} as a Lucene's {@link Document}.
	 * 
	 * @param decoratedColumn
	 *            the {@link DecoratedColumn} to be formatted as a Lucene's {@link Document}.
	 * @return the specified {@link DecoratedColumn} as a Lucene's {@link Document}.
	 */
	public Document document(DecoratedColumn decoratedColumn) {
		Document document = new Document();
		document.add(fullKeyMapper.field(decoratedColumn));
		document.add(tokenMapper.field(decoratedColumn));
		document.add(partitionKeyMapper.field(decoratedColumn));
		document.add(clusteringKeyMapper.field(decoratedColumn));
		document.add(valueMapper.field(decoratedColumn));
		document.add(timestampMapper.field(decoratedColumn));
		return document;
	}
	
	public ByteBuffer partitonKey(Document document) {
		return partitionKeyMapper.bytes(document);
	}
	
	public ByteBuffer clusteringKey(Document document) {
		return clusteringKeyMapper.bytes(document);
	}
	
	public Term term(DecoratedColumn decoratedColumn) {
		return fullKeyMapper.term(decoratedColumn);
	}
	
	public Query query(ByteBuffer value) {
		return valueMapper.query(value);
	}
	
	public Query queryBefore(long timestamp) {
		return timestampMapper.query(null, timestamp, false, true);
	}

	public Sort sort() {
		SortField tokenSortField = tokenMapper.sort();
		SortField nameSortField = clusteringKeyMapper.sort();
		return new Sort(tokenSortField, nameSortField);
	}

}
