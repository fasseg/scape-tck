package eu.scapeproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.metadata.dc.DCMetadata;

public class LuceneIndex {
    private final Directory dir = new RAMDirectory();
    private IndexWriter writer;
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndex.class);

    public void addEntity(IntellectualEntity entity) throws IOException {
        LOG.info(":: Adding entity " + entity.getIdentifier().getValue());
        Document doc = new Document();
        doc.add(new Field("id", entity.getIdentifier().getValue(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        DCMetadata dc=(DCMetadata) entity.getDescriptive();
        if (dc != null){
            if(dc.getTitle() != null && dc.getTitle().get(0)!= null){
                doc.add(new Field("title",dc.getTitle().get(0), Field.Store.YES, Field.Index.ANALYZED));
            }
            if (dc.getDescription() != null && dc.getDescription().get(0)!= null){
                doc.add(new Field("description", dc.getDescription().get(0), Field.Store.YES, Field.Index.ANALYZED));
            }
        }
        getIndexWriter().addDocument(doc);
        getIndexWriter().commit();
    }

    public List<String> searchEntity(String term) throws Exception {
        LOG.info(":: searching for " + term);
        IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
        Query query = MultiFieldQueryParser.parse(
                Version.LUCENE_36,
                new String[] { term, term, term },
                new String[] { "id", "title", "description" },
                new SimpleAnalyzer(Version.LUCENE_36));
        TopDocs hits = searcher.search(query, 10);
        List<String> result = new ArrayList<String>();
        for (ScoreDoc hit : hits.scoreDocs) {
            Document doc = searcher.doc(hit.doc);
            result.add(doc.get("id"));
        }
        searcher.close();
        LOG.info(":: search yielded " + result.size() + " hits");
        return result;
    }

    private IndexWriter getIndexWriter() throws IOException {
        if (writer == null) {
            writer = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)));
        }
        return writer;
    }

    public void close() throws IOException {
        this.writer.close();
        this.dir.close();
    }

}
