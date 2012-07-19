package eu.scapeproject;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.UUIDIdentifier;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.mets.MetsFactory;

public class ConnectorAPIMockTest {

	private static final ConnectorAPIMock MOCK = new ConnectorAPIMock();
	private static final String MOCK_URL = "http://localhost:" + MOCK.getPort() + "/";

	@BeforeClass
	public static void setup() throws Exception {
		Thread t = new Thread(MOCK);
		t.start();
		while (!MOCK.isRunning()) {
			Thread.sleep(10);
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
		MOCK.stop();
		MOCK.purgeStorage();
		assertFalse(MOCK.isRunning());
	}

	@Test
	public void testIngestEmptyIntelletualEntity() throws Exception {
		IntellectualEntity ie = new IntellectualEntity.Builder()
				.identifier(new UUIDIdentifier())
				.descriptive(new DCMetadata.Builder()
						.title("A test entity")
						.date(new Date())
						.language("en")
						.build())
				.build();
		HttpResponse resp = HttpUtil.getInstance().postEntity(ie);
		assertTrue(resp.getStatusLine().getStatusCode() == 201);
		resp = HttpUtil.getInstance().getEntity(ie.getIdentifier().getValue());
		IOUtils.copy(resp.getEntity().getContent(),System.out);
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
	}

}
