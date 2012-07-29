package eu.scapeproject;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.core.spi.LifeCycle;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.LifecycleState;
import eu.scapeproject.model.LifecycleState.State;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.mets.MetsMarshaller;

public class ConnectorAPIMockTest {

	private static final ConnectorAPIMock MOCK = new ConnectorAPIMock();
	private static final String MOCK_URL = "http://localhost:" + MOCK.getPort() + "/";
	private static final HttpClient CLIENT = new DefaultHttpClient();

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
		MOCK.close();
		assertFalse(MOCK.isRunning());
	}

	@Test
	public void testInvalidIntellectualEntity() throws Exception {
		HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity("non-existant");
		HttpResponse resp = CLIENT.execute(get);
		assertTrue(resp.getStatusLine().getStatusCode() == 404);
		get.releaseConnection();
	}

	@Test
	public void retrieveVersionList() throws Exception {
		IntellectualEntity version1 = new IntellectualEntity.Builder()
				.identifier(new Identifier(UUID.randomUUID().toString()))
				.descriptive(new DCMetadata.Builder()
						.title("A test entity")
						.date(new Date())
						.language("en")
						.build())
				.build();
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(version1);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 201);
		
		IntellectualEntity version2 = new IntellectualEntity.Builder(version1)
				.build();
		HttpPut put = ConnectorAPIUtil.getInstance().createPutEntity(version1);
		resp = CLIENT.execute(put);
		put.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		
		HttpGet get = ConnectorAPIUtil.getInstance().createGetVersionList(version1.getIdentifier().getValue());
		resp = CLIENT.execute(get);
		IOUtils.copy(resp.getEntity().getContent(), System.out);
		get.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
	}

	@Test
	public void updateIntellectualEntity() throws Exception {
		IntellectualEntity version1 = new IntellectualEntity.Builder()
				.identifier(new Identifier(UUID.randomUUID().toString()))
				.descriptive(new DCMetadata.Builder()
						.title("A test entity")
						.date(new Date())
						.language("en")
						.build())
				.build();
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(version1);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 201);
		IntellectualEntity version2 = new IntellectualEntity.Builder(version1)
				.build();
		HttpPut put = ConnectorAPIUtil.getInstance().createPutEntity(version1);
		resp = CLIENT.execute(put);
		put.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
	}

	@Test
	public void testIngestMinimalIntellectualEntity() throws Exception {
		IntellectualEntity ie = new IntellectualEntity.Builder()
				.identifier(new Identifier(UUID.randomUUID().toString()))
				.descriptive(new DCMetadata.Builder()
						.title("A test entity")
						.date(new Date())
						.language("en")
						.build())
				.build();
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(ie);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 201);

		HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(ie.getIdentifier().getValue());
		resp = CLIENT.execute(get);
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		get.releaseConnection();
	}

	@Test
	public void testIngestMinimalIntellectualEntityAsynchronously() throws Exception {
		IntellectualEntity ie = new IntellectualEntity.Builder()
				.identifier(new Identifier(UUID.randomUUID().toString()))
				.descriptive(new DCMetadata.Builder()
						.title("A test entity")
						.date(new Date())
						.language("en")
						.build())
				.build();
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntityAsync(ie);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 200);

		// check the lifecyclestate
		HttpGet get=ConnectorAPIUtil.getInstance().createGetEntityLifecycleState(ie.getIdentifier().getValue());
		resp=CLIENT.execute(get);
		LifecycleState lifecycle=(LifecycleState) MetsMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
		get.releaseConnection();
		assertTrue(lifecycle.getState() == State.INGESTING);
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		
		// wait for the state to change for 15 secs, then throw an exception
		long timeStart=System.currentTimeMillis();
		while (lifecycle.getState() != State.INGESTED){
			int elapsed=(int) ((System.currentTimeMillis() - timeStart) / 1000D);
			if (elapsed > 15) {
				fail("timeout while asynchronously ingesting object");
			}
			System.out.println("waiting for ingestion. " + elapsed + " seconds passed");
			Thread.sleep(1000);
			get=ConnectorAPIUtil.getInstance().createGetEntityLifecycleState(ie.getIdentifier().getValue());
			resp=CLIENT.execute(get);
			lifecycle=(LifecycleState) MetsMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
		}
	}

	@Test
	public void testRetrieveIntellectualEntityWithRefs() throws Exception {
		IntellectualEntity ie = new IntellectualEntity.Builder()
				.identifier(new Identifier(UUID.randomUUID().toString()))
				.descriptive(new DCMetadata.Builder()
						.title("A test entity")
						.date(new Date())
						.language("en")
						.build())
				.build();
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(ie);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 201);

		HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(ie.getIdentifier().getValue(), true);
		resp = CLIENT.execute(get);
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		MetsDocument doc = (MetsDocument) MetsMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
		assertTrue(doc.getDmdSec() != null);
		assertTrue(doc.getDmdSec().getMetadataReference() != null);
		assertTrue(doc.getDmdSec().getMetadataWrapper() == null);
		get.releaseConnection();
	}

	@Test
	public void testIngestImage() throws Exception {
		IntellectualEntity entity = ModelUtil.createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
				.create("https://a248.e.akamai.net/assets.github.com/images/modules/about_page/octocat.png?1315937507"))));
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 201);

		HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(entity.getIdentifier().getValue());
		resp = CLIENT.execute(get);
		IntellectualEntity fetched = MetsMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		get.releaseConnection();
		assertTrue(fetched.getLifecycleState().getState().equals(State.INGESTED));
	}

	@Test
	public void retrieveFile() throws Exception{
		IntellectualEntity entity = ModelUtil.createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
				.create("https://a248.e.akamai.net/assets.github.com/images/modules/about_page/octocat.png?1315937507"))));
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
		HttpResponse resp = CLIENT.execute(post);
		post.releaseConnection();
		assertTrue(resp.getStatusLine().getStatusCode() == 201);

		HttpGet get = ConnectorAPIUtil.getInstance().createGetFile(entity.getRepresentations().get(0).getFiles().iterator().next());
		resp=CLIENT.execute(get);
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		String xml=IOUtils.toString(resp.getEntity().getContent());
		assertTrue(xml.length() > 10); //check for some content
		get.releaseConnection();
	}

	@Test
	public void testRetrieveMetadataRecord() throws Exception {
		IntellectualEntity entity = ModelUtil.createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
				.create("http://example.com/void"))));
		// post an entity without identifiers
		HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
		CLIENT.execute(post);
		post.releaseConnection();

		// fetch the entity to learn the generated idenifiers
		HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(entity.getIdentifier().getValue());
		HttpResponse resp = CLIENT.execute(get);
		IntellectualEntity fetched = MetsMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
		get.releaseConnection();

		// and try to fetch and validate the fecthed entity's metadata
		get = ConnectorAPIUtil.getInstance().createGetMetadata(fetched.getDescriptive().getId());
		resp = CLIENT.execute(get);
		assertTrue(resp.getStatusLine().getStatusCode() == 200);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		IOUtils.copy(resp.getEntity().getContent(), bos);
		get.releaseConnection();
		DCMetadata dc = MetsMarshaller.getInstance().deserialize(DCMetadata.class, new ByteArrayInputStream(bos.toByteArray()));
		assertEquals(entity.getDescriptive(), dc);
	}
}
