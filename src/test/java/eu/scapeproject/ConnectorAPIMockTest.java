package eu.scapeproject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.BitStream;
import eu.scapeproject.model.BitStream.Type;
import eu.scapeproject.model.File;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.IntellectualEntityCollection;
import eu.scapeproject.model.LifecycleState;
import eu.scapeproject.model.LifecycleState.State;
import eu.scapeproject.model.Representation;
import eu.scapeproject.model.VersionList;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.mets.SCAPEMarshaller;

public class ConnectorAPIMockTest {

    private static final ConnectorAPIMock MOCK = new ConnectorAPIMock();
    private static final HttpClient CLIENT = new DefaultHttpClient();
    private static final Logger log = LoggerFactory.getLogger(ConnectorAPIMockTest.class);

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
    public void testGetIntellectualEntityList() throws Exception {
        List<String> ids = new ArrayList<String>();
        // ingest entity 1
        IntellectualEntity entity1 = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(new DCMetadata.Builder()
                        .title("A test entity")
                        .date(new Date())
                        .language("en")
                        .build())
                .build();
        ids.add(entity1.getIdentifier().getValue());
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity1);
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        // ingest entity 2
        IntellectualEntity entity2 = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(new DCMetadata.Builder()
                        .title("A test entity")
                        .date(new Date())
                        .language("en")
                        .build())
                .build();
        ids.add(entity2.getIdentifier().getValue());
        post = ConnectorAPIUtil.getInstance().createPostEntity(entity2);
        resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        StringBuilder uriList = new StringBuilder();
        for (String id : ids) {
            uriList.append(id + "\n");
        }
        post = ConnectorAPIUtil.getInstance().createGetUriList(uriList.toString());
        resp = CLIENT.execute(post);
        // IOUtils.copy(resp.getEntity().getContent(), System.out);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
    }

    @Test
    public void testGetVersionList() throws Exception {
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
        HttpPut put = ConnectorAPIUtil.getInstance().createPutEntity(version2);
        resp = CLIENT.execute(put);
        put.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);

        HttpGet get = ConnectorAPIUtil.getInstance().createGetVersionList(version1.getIdentifier().getValue());
        resp = CLIENT.execute(get);
        VersionList versions = (VersionList) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
        get.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        assertTrue(versions.getVersionIdentifiers().size() == 2);
        assertTrue(versions.getEntityId().equals(version1.getIdentifier().getValue()));

    }

    @Test
    public void testIngestImage() throws Exception {
        IntellectualEntity entity = ModelUtil.createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
                .create("https://a248.e.akamai.net/assets.github.com/images/modules/about_page/octocat.png?1315937507"), null)));
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(entity.getIdentifier().getValue());
        resp = CLIENT.execute(get);
        IntellectualEntity fetched = SCAPEMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        get.releaseConnection();
        assertTrue(fetched.getLifecycleState().getState().equals(State.INGESTED));
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
        HttpGet get = ConnectorAPIUtil.getInstance().createGetEntityLifecycleState(ie.getIdentifier().getValue());
        resp = CLIENT.execute(get);
        LifecycleState lifecycle = (LifecycleState) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
        get.releaseConnection();
        assertTrue(lifecycle.getState() == State.INGESTING);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);

        // wait for the state to change for 15 secs, then throw an exception
        long timeStart = System.currentTimeMillis();
        while (lifecycle.getState() != State.INGESTED) {
            int elapsed = (int) ((System.currentTimeMillis() - timeStart) / 1000D);
            if (elapsed > 15) {
                fail("timeout while asynchronously ingesting object");
            }
            log.info("TEST: waiting for ingestion. " + elapsed + " seconds passed");
            Thread.sleep(1000);
            get = ConnectorAPIUtil.getInstance().createGetEntityLifecycleState(ie.getIdentifier().getValue());
            resp = CLIENT.execute(get);
            lifecycle = (LifecycleState) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
        }
    }

    @Test
    public void testInvalidIntellectualEntity() throws Exception {
        HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity("non-existant");
        HttpResponse resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 404);
        get.releaseConnection();
    }

    @Test
    public void testRetrieveBitStream() throws Exception {
        BitStream bs = new BitStream.Builder()
                .type(Type.STREAM)
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .build();

        IntellectualEntity entity = ModelUtil
                .createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
                        .create("https://a248.e.akamai.net/assets.github.com/images/modules/about_page/octocat.png?1315937507"),
                        Arrays.asList(bs))));
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        HttpGet get = ConnectorAPIUtil.getInstance().createGetBitStream(
                entity.getRepresentations().get(0).getFiles().get(0).getBitStreams().get(0).getIdentifier().getValue());
        resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        BitStream fetched = SCAPEMarshaller.getInstance().deserialize(BitStream.class, resp.getEntity().getContent());
        get.releaseConnection();
        assertEquals(entity.getRepresentations().get(0).getFiles().get(0).getBitStreams().get(0), fetched); }

    @Test
    public void testRetrieveFile() throws Exception {
        IntellectualEntity entity = ModelUtil.createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
                .create("https://a248.e.akamai.net/assets.github.com/images/modules/about_page/octocat.png?1315937507"), null)));
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        HttpGet get = ConnectorAPIUtil.getInstance().createGetFile(entity.getRepresentations().get(0).getFiles().iterator().next());
        resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        String xml = IOUtils.toString(resp.getEntity().getContent());
        assertTrue(xml.length() > 10); // check for some content
        get.releaseConnection();
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
        MetsDocument doc = (MetsDocument) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(resp.getEntity().getContent());
        assertTrue(doc.getDmdSec() != null);
        assertTrue(doc.getDmdSec().getMetadataReference() != null);
        assertTrue(doc.getDmdSec().getMetadataWrapper() == null);
        get.releaseConnection();
    }

    @Test
    public void testRetrieveMetadataRecord() throws Exception {
        IntellectualEntity entity = ModelUtil.createEntity(Arrays.asList(ModelUtil.createImageRepresentation(URI
                .create("http://example.com/void"), null)));
        // post an entity without identifiers
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(entity);
        CLIENT.execute(post);
        post.releaseConnection();

        // fetch the entity to learn the generated idenifiers
        HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(entity.getIdentifier().getValue());
        HttpResponse resp = CLIENT.execute(get);
        IntellectualEntity fetched = SCAPEMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
        get.releaseConnection();

        // and try to fetch and validate the fecthed entity's metadata
        get = ConnectorAPIUtil.getInstance().createGetMetadata(fetched.getDescriptive().getId());
        resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(resp.getEntity().getContent(), bos);
        get.releaseConnection();
        DCMetadata dc = SCAPEMarshaller.getInstance().deserialize(DCMetadata.class, new ByteArrayInputStream(bos.toByteArray()));
        assertEquals(entity.getDescriptive(), dc);
    }

    @Test
    public void testRetrieveRepresentation() throws Exception {
        Representation rep = ModelUtil.createTestRepresentation("test-representation-" + System.currentTimeMillis());

        // ingest the entity with it's representation for later fetching
        IntellectualEntity.Builder ie = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .representations(Arrays.asList(rep))
                .descriptive(new DCMetadata.Builder()
                        .title("entity with representation")
                        .description("description of entity")
                        .date(new Date())
                        .language("en")
                        .build());
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(ie.build());
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        // and fetch the ingested representation
        HttpGet get = ConnectorAPIUtil.getInstance().createGetRepresentation(rep.getIdentifier().getValue());
        resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        IntellectualEntity entity = SCAPEMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
        get.releaseConnection();
        assertEquals(rep, entity.getRepresentations().get(0));
    }

    @Test
    public void testSearchEntity() throws Exception {
        // ingest an entity to search in
        IntellectualEntity.Builder ie = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(new DCMetadata.Builder()
                        .title("A test entity")
                        .description("this should yield a hit!")
                        .date(new Date())
                        .language("en")
                        .build());
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(ie.build());
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        // and search for the ingested entity
        HttpGet get = ConnectorAPIUtil.getInstance().createGetSRUEntity("should");
        resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        IntellectualEntityCollection resultSet = (IntellectualEntityCollection) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(
                resp.getEntity().getContent());
        get.releaseConnection();
        assertTrue(resultSet.getEntities().size() == 1);
        IntellectualEntity searched = SCAPEMarshaller.getInstance().deserializeEntity(resultSet.getEntities().get(0));
        assertEquals(ie.lifecycleState(new LifecycleState("", State.INGESTED)).build(), searched);
    }

    @Test
    public void testSearchRepresentation() throws Exception {
        // ingest an entity to search in
        IntellectualEntity.Builder ie = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .representations(Arrays.asList(ModelUtil.createTestRepresentation("testingestrepresentation-" + System.currentTimeMillis())))
                .descriptive(new DCMetadata.Builder()
                        .title("entity for testing representations")
                        .description("purely for testing purposes")
                        .date(new Date())
                        .language("en")
                        .build());
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(ie.build());
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        // and search for the ingested entity
        HttpGet get = ConnectorAPIUtil.getInstance().createGetSRUrepresentation("testingestrepresentation");
        resp = CLIENT.execute(get);
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
        IntellectualEntityCollection resultSet = (IntellectualEntityCollection) SCAPEMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(
                resp.getEntity().getContent());
        get.releaseConnection();
        assertTrue(resultSet.getEntities().size() == 1);
        IntellectualEntity searched = SCAPEMarshaller.getInstance().deserializeEntity(resultSet.getEntities().get(0));
        assertEquals(ie.lifecycleState(new LifecycleState("", State.INGESTED)).build(), searched);
    }

    @Test
    public void testUpdateIntellectualEntity() throws Exception {
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
        HttpPut put = ConnectorAPIUtil.getInstance().createPutEntity(version2);
        resp = CLIENT.execute(put);
        put.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
    }

    @Test
    public void testUpdateIntellectualRepresentation() throws Exception {
        Representation oldRep = ModelUtil.createTestRepresentation("ye olde representation");
        // create an intellectual entity with a representation
        IntellectualEntity oldVersion = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .representations(Arrays.asList(oldRep))
                .descriptive(new DCMetadata.Builder()
                        .title("A test entity")
                        .date(new Date())
                        .language("en")
                        .build())
                .build();
        // post it for persisting to the Mock
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(oldVersion);
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        // create an an updated representation send a post to persist the
        // changes
        Representation newRep = new Representation.Builder(oldRep)
                .title("The Brand-New Representation")
                .build();

        HttpPut put = ConnectorAPIUtil.getInstance().createPutRepresentation(newRep);
        resp = CLIENT.execute(put);
        put.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);

        // get the update entity and check that the title has changed
        HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(oldVersion.getIdentifier().getValue());
        resp = CLIENT.execute(get);
        IntellectualEntity newVersion = SCAPEMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
        assertEquals(newRep, newVersion.getRepresentations().get(0));
        get.releaseConnection();
    }

    @Test
    public void testUpdateMetadata() throws Exception {
        DCMetadata.Builder dc = new DCMetadata.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .title("Ye olde dc record")
                .date(new Date())
                .language("en");
        IntellectualEntity oldVersion = new IntellectualEntity.Builder()
                .identifier(new Identifier(UUID.randomUUID().toString()))
                .descriptive(dc.build())
                .build();
        // post it for persisting to the Mock
        HttpPost post = ConnectorAPIUtil.getInstance().createPostEntity(oldVersion);
        HttpResponse resp = CLIENT.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        // update the metadata in order to check the PUT method
        dc.title("The Brand-New DC record");
        HttpPut put = ConnectorAPIUtil.getInstance().createPutMetadata(dc.build());
        resp = CLIENT.execute(put);
        put.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);

        // fetch the entity and check for the updated metadata record
        HttpGet get = ConnectorAPIUtil.getInstance().createGetEntity(oldVersion.getIdentifier().getValue());
        resp = CLIENT.execute(get);
        IntellectualEntity newVersion = SCAPEMarshaller.getInstance().deserialize(IntellectualEntity.class, resp.getEntity().getContent());
        get.releaseConnection();
        assertEquals(newVersion.getDescriptive(), dc.build());

    }

}
