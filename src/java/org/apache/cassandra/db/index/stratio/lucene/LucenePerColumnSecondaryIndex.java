/**
 * 
 */
package org.apache.cassandra.db.index.stratio.lucene;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.PerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.index.stratio.ByteBufferUtils;
import org.apache.cassandra.db.index.stratio.DecoratedColumn;
import org.apache.cassandra.db.index.stratio.lucene.mapping.ColumnMapper;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

/**
 * @author adelapena
 * 
 */
public class LucenePerColumnSecondaryIndex extends PerColumnSecondaryIndex {

	private SecondaryIndexManager secondaryIndexManager;
	private CFMetaData tableMetadata;
	private ColumnDefinition columnDefinition;

	private AbstractType<?> keyComparator;
	private CompositeType nameComparator;

	private String ksName;
	private String cfName;
	private String columnName;
	private String indexName;

	private ColumnMapper columnMapper;
	private LuceneIndex luceneIndex;

	private ByteBuffer lastColumnName;

	private boolean isRegularColumn;

	@Override
	public void init() {

		// Load column family info
		secondaryIndexManager = baseCfs.indexManager;
		tableMetadata = baseCfs.metadata;
		columnDefinition = columnDefs.iterator().next();
		ksName = tableMetadata.ksName;
		cfName = tableMetadata.cfName;
		columnName = CFDefinition.definitionType.getString(columnDefinition.name);
		indexName = columnDefinition.getIndexName();
		isRegularColumn = columnDefinition.type.equals(ColumnDefinition.Type.REGULAR);

		// Get validators
		keyComparator = baseCfs.metadata.getKeyValidator();
		nameComparator = (CompositeType) baseCfs.getComparator();

		// Build Lucene's directory path
		String[] dataFileLocations = DatabaseDescriptor.getAllDataFileLocations();
		String indexesDirectory = dataFileLocations[0] + File.separatorChar + "lucene";
		String kespaceIndexesDirectory = indexesDirectory + File.separatorChar + cfName;
		String directoryPath = kespaceIndexesDirectory + File.separatorChar + indexName;

		// Build Lucen's stuff
		columnMapper = new ColumnMapper(columnDefinition);
		luceneIndex = new LuceneIndex(directoryPath);

		// Get last column name for building slice ranges
		for (ColumnDefinition columnDefinition : baseCfs.metadata.regularColumns()) {
			ByteBuffer rawColumnName = columnDefinition.name;
			if (lastColumnName == null || compareColumnNames(lastColumnName, rawColumnName) < 0) {
				lastColumnName = rawColumnName;
			}
		}

		// Log index creation
		logger.info(format("Init in %s", directoryPath));
	}

	private String format(String message, Object... options) {
		return String.format("Lucene per column index %s.%s.%s.%s : %s",
		                     ksName,
		                     cfName,
		                     columnName,
		                     indexName,
		                     String.format(message, options));
	}

	private static int compareColumnNames(ByteBuffer o1, ByteBuffer o2) {
		return UTF8Type.instance.compare(o1, o2);
	}

	@Override
	public boolean indexes(ByteBuffer columnName) {
		ByteBuffer[] components = nameComparator.split(columnName);
		ByteBuffer lastComponent = components[components.length - 1];
		if (!lastComponent.hasRemaining()) { // Is clustering key
			return !isRegularColumn;
		} else {
			return lastComponent.equals(columnDefinition.name);
		}
	}

	@Override
	public void delete(ByteBuffer partitionKey, Column column) {
		DecoratedColumn decoratedColumn = decorate(partitionKey, column);
		Term term = columnMapper.term(decoratedColumn);
		luceneIndex.delete(term);
		logger.info(format("Deleted column %s", decoratedColumn));
	}

	@Override
	public void insert(ByteBuffer partitionKey, Column column) {
		DecoratedColumn decoratedColumn = decorate(partitionKey, column);
		Document document = columnMapper.document(decoratedColumn);
		luceneIndex.insert(document);
		logger.info(format("Inserted column %s", decoratedColumn));
	}

	@Override
	public void update(ByteBuffer partitionKey, Column column) {
		DecoratedColumn decoratedColumn = decorate(partitionKey, column);
		Term term = columnMapper.term(decoratedColumn);
		Document document = columnMapper.document(decoratedColumn);
		luceneIndex.update(term, document);
		logger.info(format("Updated column %s ", decoratedColumn));
	}

	private DecoratedColumn decorate(ByteBuffer partitionKey, Column column) {
		return new DecoratedColumn(partitionKey, column, baseCfs, columnDefinition);
	}

	@Override
	public void reload() {
		logger.info(format("Reloading"));
		luceneIndex.commit();
	}

	@Override
	public void validateOptions() throws ConfigurationException {
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
		logger.info(format("Creating searcher"));
		return new LucenePerColumnSecondaryIndexSearcher(secondaryIndexManager,
		                                                 this,
		                                                 columns,
		                                                 columnMapper,
		                                                 luceneIndex);
	}

	@Override
	public void forceBlockingFlush() {
		logger.info(format("Flushing"));
		luceneIndex.commit();
	}

	@Override
	public long getLiveSize() {
		return luceneIndex.getRAMSizeInBytes();
	}

	@Override
	public ColumnFamilyStore getIndexCfs() {
		return null;
	}

	@Override
	public void removeIndex(ByteBuffer columnName) {
		logger.info(format("Removing"));
		luceneIndex.removeIndex();
	}

	@Override
	public void invalidate() {
		logger.info(format("Invalidating"));
	}

	@Override
	public void truncateBlocking(long truncatedAt) {
		logger.info(format("Truncating"));

		Query query = columnMapper.queryBefore(truncatedAt);
		luceneIndex.delete(query);
	}

	public Row getRow(ByteBuffer partitionKey, ByteBuffer clusteringKey) {

		ByteBuffer[] clusteringKeyComponents = ByteBufferUtils.split(clusteringKey, nameComparator);
		ByteBuffer[] finishComponents = new ByteBuffer[clusteringKeyComponents.length];
		for (int i = 0; i < clusteringKeyComponents.length - 1; i++) {
			finishComponents[i] = clusteringKeyComponents[i];
		}
		finishComponents[clusteringKeyComponents.length - 1] = lastColumnName;
		ByteBuffer finishColumnName = CompositeType.build(finishComponents);

		DecoratedKey decoratedKey = baseCfs.partitioner.decorateKey(partitionKey);
		long timestamp = System.currentTimeMillis(); // Current time
		QueryFilter f = QueryFilter.getSliceFilter(decoratedKey,
		                                           cfName,
		                                           clusteringKey,
		                                           finishColumnName,
		                                           false,
		                                           Integer.MAX_VALUE,
		                                           timestamp);
		ColumnFamily cf = baseCfs.getColumnFamily(f);
		Row row = new Row(decoratedKey, cf);
		return row;
	}

}
