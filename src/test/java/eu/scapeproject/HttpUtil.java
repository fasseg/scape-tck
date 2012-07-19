package eu.scapeproject;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.mets.MetsFactory;

public class HttpUtil {
    public static void postEntity(String path,IntellectualEntity ie) throws Exception{
        HttpClient client=new DefaultHttpClient();
        HttpPost post=new HttpPost(path);
        MetsFactory factory=MetsFactory.getInstance();
        ByteArrayOutputStream bos=new ByteArrayOutputStream();
        factory.serialize(ie, bos);
        post.setEntity(new ByteArrayEntity(bos.toByteArray()));
        HttpResponse resp=client.execute(post);
        assertTrue(resp.getStatusLine().getStatusCode() == 201);
    }

}
