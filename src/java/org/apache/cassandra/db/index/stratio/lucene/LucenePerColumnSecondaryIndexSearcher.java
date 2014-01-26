package org.apache.cassandra.db.index.stratio.lucene;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.index.stratio.lucene.mapping.ColumnMapper;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

public class LucenePerColumnSecondaryIndexSearcher extends SecondaryIndexSearcher {

	private final ColumnMapper luceneMapper;
	private final LuceneIndex luceneIndex;
	
	private final LucenePerColumnSecondaryIndex currentIndex;

	public LucenePerColumnSecondaryIndexSearcher(SecondaryIndexManager indexManager,
	                                             LucenePerColumnSecondaryIndex currentIndex,
	                                             Set<ByteBuffer> columns,ColumnMapper luceneMapper,
	                                             LuceneIndex luceneIndex) {
		super(indexManager, columns);
		this.currentIndex = currentIndex;
		this.luceneMapper = luceneMapper;
		this.luceneIndex = luceneIndex;
	}

	@Override
	public List<Row> search(ExtendedFilter filter) {
		IndexExpression indexExpression = filter.getClause().get(0);
		ByteBuffer columnValue = indexExpression.value;
		Query query = luceneMapper.query(columnValue);
		List<Document> documents = luceneIndex.search(query, 100, luceneMapper.sort());
		List<org.apache.cassandra.db.Row> rows = new LinkedList<>();
		for (Document document : documents) {
			ByteBuffer partitionKey = luceneMapper.partitonKey(document);
			ByteBuffer clusteringKey = luceneMapper.clusteringKey(document);
			Row row = currentIndex.getRow(partitionKey, clusteringKey);
			rows.add(row);
		}
		return rows;
	}

	@Override
	public boolean isIndexing(List<IndexExpression> clause) {
		System.out.println("isIndexing(" + clause + ")");
		return highestSelectivityPredicate(clause) != null;
	}

	@Override
	protected IndexExpression highestSelectivityPredicate(List<IndexExpression> clause) { // TODO: Check
		System.out.println("highestSelectivityPredicate(" + clause + ")");
		for (IndexExpression expression : clause) {
			SecondaryIndex index = indexManager.getIndexForColumn(expression.column_name);
			if (index != null && expression.op == IndexOperator.EQ && index == currentIndex) {
				return expression;
			} else {
				continue;
			}
		}
		return null;
	}
	
}
