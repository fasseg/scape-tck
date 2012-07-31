package eu.scapeproject;

import java.io.ByteArrayOutputStream;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import eu.scapeproject.dto.mets.MetsMetadata;
import eu.scapeproject.model.File;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.Representation;
import eu.scapeproject.model.mets.SCAPEMarshaller;

public class ConnectorAPIUtil {
    private static final String MOCK_URL = "http://localhost:8783";
    private static final String ENTITY_PATH = "/entity";
    private static final String ENTITY_LIST_PATH = "/entity-list";
    private static final String ENTITY_ASYNC_PATH = "/entity-async";
    private static final String ENTITY_SRU_PATH = "/sru/entities";
    private static final String ENTITY_VERSION_LIST_PATH = "/entity-version-list";
    private static final String REPRESENTATION_PATH = "/representation/";
    private static final String REPRESENTATION_SRU_PATH = "/sru/representations";
    private static final String FILE_PATH = "/file";
    private static final String BITSTREAM_PATH = "/bitstream";
    private static final String METADATA_PATH = "/metadata";
    private static final String LIFECYCLE_STATE_PATH = "/lifecycle";

    private static ConnectorAPIUtil INSTANCE;

    public static ConnectorAPIUtil getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ConnectorAPIUtil();
        }
        return INSTANCE;
    }

    private ConnectorAPIUtil() {
        super();
    }

    public HttpGet createGetEntity(String id) throws Exception {
        return new HttpGet(MOCK_URL + ENTITY_PATH + "/" + id);
    }

    public HttpGet createGetEntity(String id, boolean references) throws Exception {
        return new HttpGet(MOCK_URL + ENTITY_PATH + "/" + id + "?useReferences=" + references);
    }

    public HttpGet createGetEntityLifecycleState(String id) {
        return new HttpGet(MOCK_URL + LIFECYCLE_STATE_PATH + "/" + id);
    }

    public HttpGet createGetFile(File next) {
        return new HttpGet(MOCK_URL + FILE_PATH + "/" + next.getIdentifier().getValue());
    }

    public HttpGet createGetMetadata(String id) throws Exception {
        return new HttpGet(MOCK_URL + METADATA_PATH + "/" + id);
    }

    public HttpGet createGetRepresentation(String id) {
        return new HttpGet(MOCK_URL + REPRESENTATION_PATH + "/" + id);
    }

    public HttpGet createGetSRUEntity(String term) {
        // TODO: Schema for entitylists
        // TODO: use CQL
        return new HttpGet(MOCK_URL + ENTITY_SRU_PATH + "?operation=searchRetrieve&query=" + term + "&recordPacking=xml&recordSchema=entitylist.xsd");
    }

    public HttpGet createGetSRUrepresentation(String term) {
        // TODO Schema for representations
        // TODO: use CQL, not only the term
        return new HttpGet(MOCK_URL + REPRESENTATION_SRU_PATH + "?operation=searchRetrieve&query=" + term + "&recordPacking=xml&recordSchema=entitylist.xsd");
    }

    public HttpPost createGetUriList(String string) {
        HttpPost post = new HttpPost(MOCK_URL + ENTITY_LIST_PATH);
        post.setEntity(new ByteArrayEntity(string.getBytes()));
        return post;
    }

    public HttpGet createGetVersionList(String id) {
        return new HttpGet(MOCK_URL + ENTITY_VERSION_LIST_PATH + "/" + id);
    }

    public HttpPost createPostEntity(IntellectualEntity ie) throws Exception {
        HttpPost post = new HttpPost(MOCK_URL + ENTITY_PATH);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        SCAPEMarshaller.getInstance().serialize(ie, bos);
        post.setEntity(new ByteArrayEntity(bos.toByteArray()));
        return post;
    }

    public HttpPost createPostEntityAsync(IntellectualEntity ie) throws Exception {
        HttpPost post = new HttpPost(MOCK_URL + ENTITY_ASYNC_PATH);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        SCAPEMarshaller.getInstance().serialize(ie, bos);
        post.setEntity(new ByteArrayEntity(bos.toByteArray()));
        return post;
    }

    public HttpPut createPutEntity(IntellectualEntity ie) throws Exception {
        HttpPut put = new HttpPut(MOCK_URL + ENTITY_PATH + "/" + ie.getIdentifier().getValue());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        SCAPEMarshaller.getInstance().serialize(ie, bos);
        put.setEntity(new ByteArrayEntity(bos.toByteArray()));
        return put;
    }

    public HttpPut createPutMetadata(MetsMetadata data) throws Exception{
        HttpPut put=new HttpPut(MOCK_URL + METADATA_PATH + "/" + data.getId());
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        SCAPEMarshaller.getInstance().serialize(data, bos);
        put.setEntity(new ByteArrayEntity(bos.toByteArray()));
        return put;
    }

    public HttpPut createPutRepresentation(Representation newRep) throws Exception {
        HttpPut put=new HttpPut(MOCK_URL + REPRESENTATION_PATH + "/" + newRep.getIdentifier().getValue());
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        SCAPEMarshaller.getInstance().serialize(newRep, bos);
        put.setEntity(new ByteArrayEntity(bos.toByteArray()));
        return put;
    }

	public HttpGet createGetBitStream(String id) {
        return new HttpGet(MOCK_URL + BITSTREAM_PATH + "/" + id);
	}
}
