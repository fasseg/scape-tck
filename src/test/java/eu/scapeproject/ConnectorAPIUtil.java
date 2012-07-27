package eu.scapeproject;

import java.io.ByteArrayOutputStream;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.mets.MetsMarshaller;

public class ConnectorAPIUtil {
	private static final String MOCK_URL="http://localhost:8783";
	private static final String ENTITY_PATH="/entity";
	private static final String METADATA_PATH="/metadata";
	private static final String ENTITY_VERSION_LIST_PATH="/entity-version-list";
	private static ConnectorAPIUtil INSTANCE;

	private ConnectorAPIUtil(){
		super();
	}
	
	public static ConnectorAPIUtil getInstance() {
		if (INSTANCE == null){
			INSTANCE = new ConnectorAPIUtil();
		}
		return INSTANCE;
	}

	public HttpPost createPostEntity(IntellectualEntity ie) throws Exception{
        HttpPost post=new HttpPost(MOCK_URL + ENTITY_PATH);
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        MetsMarshaller.getInstance().serialize(ie, bos);
        post.setEntity(new ByteArrayEntity(bos.toByteArray()));
        return post;
    }

	public HttpGet createGetEntity(String id,boolean references) throws Exception{
		return new HttpGet(MOCK_URL + ENTITY_PATH + "/" + id + "?useReferences=" + references);
	}
	
	public HttpGet createGetEntity(String id) throws Exception{
		return new HttpGet(MOCK_URL + ENTITY_PATH + "/" + id);
	}

	public HttpGet createGetMetadata(String id) throws Exception{
		return new HttpGet(MOCK_URL + METADATA_PATH + "/" + id);
	}

	public HttpPut createPutEntity(IntellectualEntity ie) throws Exception{
		HttpPut put=new HttpPut(MOCK_URL + ENTITY_PATH + "/" + ie.getIdentifier().getValue());
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        MetsMarshaller.getInstance().serialize(ie, bos);
        put.setEntity(new ByteArrayEntity(bos.toByteArray()));
		return put;
	}

	public HttpGet createGetVersionList(String id) {
		return new HttpGet(MOCK_URL + ENTITY_VERSION_LIST_PATH + "/" + id);
	}

}
