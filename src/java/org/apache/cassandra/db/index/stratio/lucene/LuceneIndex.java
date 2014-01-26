package org.apache.cassandra.db.index.stratio.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class LuceneIndex {

	private File file;
	private Directory directory;
	private Analyzer analyzer;
	private IndexWriter indexWriter;
	private TrackingIndexWriter trackingIndexWriter;
	private SearcherManager searcherManager;
	private ControlledRealTimeReopenThread<IndexSearcher> indexSearcherReopenThread;

	public LuceneIndex(String path) {
		try {

			// Get directory file
			file = new File(path);

			// directory = new RAMDirectory();
			directory = FSDirectory.open(file);

			analyzer = new EnglishAnalyzer(Version.LUCENE_46);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
			config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
			indexWriter = new IndexWriter(directory, config);

			// [2a]: Create the TrackingIndexWriter to track changes to the delegated previously
			// created IndexWriter
			trackingIndexWriter = new TrackingIndexWriter(indexWriter);

			// [2b]: Create an IndexSearcher ReferenceManager to safelly share IndexSearcher
			// instances across multiple threads
			searcherManager = new SearcherManager(indexWriter, true, null);

			// [3]: Create the ControlledRealTimeReopenThread that reopens the index periodically
			// having into
			// account the changes made to the index and tracked by the TrackingIndexWriter instance
			// The index is refreshed every 60sc when nobody is waiting and every 100 millis
			// whenever is someone waiting (see search method)
			indexSearcherReopenThread = new ControlledRealTimeReopenThread<IndexSearcher>(trackingIndexWriter,
			                                                                              searcherManager,
			                                                                              1.00,
			                                                                              0.1);
			indexSearcherReopenThread.start(); // start the refresher thread

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Inserts the specified {@link Document}.
	 * 
	 * @param document
	 *            the {@link Document} to be inserted.
	 */
	public void insert(Document document) {
		try {
			indexWriter.addDocument(document);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Updates the specified {@link Document} by first deleting the document(s) containing
	 * <code>term</code> and then adding the new document. The delete and then add are atomic as
	 * seen by a reader on the same index (flush may happen only after the add).
	 * 
	 * @param term
	 *            the {@link Term} to identify the document(s) to be deleted.
	 * @param document
	 *            the {@link Document} to be added.
	 */
	public void update(Term term, Document document) {
		try {
			indexWriter.updateDocument(term, document);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Deletes all the {@link Document}s containing the specified {@link Term}.
	 * 
	 * @param term
	 *            the {@link Term} to identify the documents to be deleted
	 */
	public void delete(Term term) {
		try {
			indexWriter.deleteDocuments(term);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean delete(Query query) {
		try {
			indexWriter.deleteDocuments(query);
			return true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commits the pending changes.
	 */
	public void commit() {
		try {
			indexWriter.commit();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Commits all changes to an index, waits for pending merges to complete, and closes all
	 * associated resources.
	 */
	public void close() throws IOException {
			indexSearcherReopenThread.interrupt();
			searcherManager.close();
			indexWriter.close();
			directory.close();
			analyzer.close();
	}

	public boolean removeIndex() {
		try {
			close();
			FileUtils.deleteRecursive(file);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	public long getRAMSizeInBytes() {
		return indexWriter == null ? 0 : indexWriter.ramSizeInBytes();
	}

	public List<Document> search(Query query, int count, Sort sort) {
		try {
			 IndexSearcher indexSearcher = searcherManager.acquire();
			try {
				TopDocs topDocs;
				if (sort == null) {
					topDocs = indexSearcher.search(query, count);
				} else {
					topDocs = indexSearcher.search(query, count, sort);
				}
				ScoreDoc[] scoreDocs = topDocs.scoreDocs;

				// Get the document keys from query result
				List<Document> documents = new ArrayList<Document>(topDocs.totalHits);
				for (ScoreDoc scoreDoc : scoreDocs) {
					Document document = indexSearcher.doc(scoreDoc.doc);
					documents.add(document);
				}
				return documents;
			} finally {
				 searcherManager.release(indexSearcher);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
