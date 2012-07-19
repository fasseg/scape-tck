package eu.scapeproject;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.mets.MetsFactory;

public class HttpUtil {
	private static final String MOCK_URL="http://localhost:8783";
	private static final String ENTITY_PATH="/entity";
	private static HttpUtil INSTANCE;

	private final HttpClient CLIENT=new DefaultHttpClient();
	
	private HttpUtil(){
		super();
	}
	
	public static HttpUtil getInstance() {
		if (INSTANCE == null){
			INSTANCE = new HttpUtil();
		}
		return INSTANCE;
	}

	public HttpResponse postEntity(IntellectualEntity ie) throws Exception{
        HttpPost post=new HttpPost(MOCK_URL + ENTITY_PATH);
        MetsFactory factory=MetsFactory.getInstance();
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        factory.serialize(ie, bos);
        post.setEntity(new ByteArrayEntity(bos.toByteArray()));
        HttpResponse resp=CLIENT.execute(post);
        post.releaseConnection();
        return resp;
    }

	public HttpResponse getEntity(String id) throws Exception{
		HttpGet get=new HttpGet(MOCK_URL + ENTITY_PATH + "/" + id);
        HttpResponse resp=CLIENT.execute(get);
        get.releaseConnection();
        return resp;
	}

}
