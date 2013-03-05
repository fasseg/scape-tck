package eu.scapeproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;

import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.MultiFieldQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.purl.dc.elements._1.ElementContainer;
import org.purl.dc.elements._1.SimpleLiteral;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.Representation;

public class LuceneIndex {
    private final Directory entityDir = new RAMDirectory();
    private final Directory representationDir = new RAMDirectory();
    private IndexWriter entityWriter;
    private IndexWriter representationWriter;
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndex.class);

    public void addEntity(IntellectualEntity entity) throws IOException {
        LOG.info("++ adding entity " + entity.getIdentifier().getValue());
        Document doc = new Document();
        doc.add(new Field("id", entity.getIdentifier().getValue(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        Object o = entity.getDescriptive();
        if (o instanceof ElementContainer){
            List<String> titles = new ArrayList<String>();
            ElementContainer dc = (ElementContainer) o;
            for (JAXBElement<?> jaxb : dc.getAny()){
                if (jaxb.getName().getLocalPart().equals("title")){
                    SimpleLiteral lit = (SimpleLiteral) jaxb.getValue();
                    for (String title : lit.getContent()){
                        doc.add(new Field("title", title, Field.Store.YES, Field.Index.ANALYZED));
                    }
                }else if (jaxb.getName().getLocalPart().equals("description")){
                    SimpleLiteral lit = (SimpleLiteral) jaxb.getValue();
                    for (String title : lit.getContent()){
                        doc.add(new Field("description", title, Field.Store.YES, Field.Index.ANALYZED));
                    }
                }
            }
        }
        if (entity.getRepresentations() != null) {
            for (Representation r : entity.getRepresentations()) {
                addRepresentation(r);
            }
        }
        getEntityWriter().addDocument(doc);
        getEntityWriter().commit();
    }

    public void addRepresentation(Representation r) throws IOException {
        LOG.info("adding representation " + r.getIdentifier() + " with title " + r.getTitle());
        Document doc = new Document();
        doc.add(new Field("id", r.getIdentifier().getValue(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        if (r.getTitle() != null) {
            doc.add(new Field("title", r.getTitle(), Field.Store.YES, Field.Index.ANALYZED));
        }
        getRepresentationWriter().addDocument(doc);
        getRepresentationWriter().commit();
    }

    public void close() throws IOException {
        if (this.entityWriter != null) {
            this.entityWriter.close();
        }
        if (this.representationWriter != null){
            this.representationWriter.close();
        }
        this.entityDir.close();
        this.representationDir.close();
    }

    private IndexWriter getEntityWriter() throws IOException {
        if (entityWriter == null) {
            entityWriter = new IndexWriter(entityDir, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)));
        }
        return entityWriter;
    }

    private IndexWriter getRepresentationWriter() throws IOException {
        if (representationWriter == null) {
            representationWriter = new IndexWriter(representationDir, new IndexWriterConfig(Version.LUCENE_36, new StandardAnalyzer(Version.LUCENE_36)));
        }
        return representationWriter;
    }

    public List<String> searchEntity(String term) throws Exception {
        LOG.info(":: searching for " + term);
        IndexSearcher searcher = new IndexSearcher(IndexReader.open(entityDir));
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

    public List<String> searchRepresentation(String term) throws Exception {
        IndexSearcher searcher = new IndexSearcher(IndexReader.open(representationDir));
        Query query = MultiFieldQueryParser.parse(
                Version.LUCENE_36,
                new String[] { term },
                new String[] { "title" },
                new SimpleAnalyzer(Version.LUCENE_36));
        TopDocs hits = searcher.search(query, 10);
        List<String> result = new ArrayList<String>();
        for (ScoreDoc hit : hits.scoreDocs) {
            Document doc = searcher.doc(hit.doc);
            result.add(doc.get("id"));
        }
        searcher.close();
        LOG.info("++ representation search yielded " + result.size() + " hits");
        return result;
    }

}
