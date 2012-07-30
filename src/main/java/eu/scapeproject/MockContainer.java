package eu.scapeproject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.dto.mets.MetsDMDSec;
import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.dto.mets.MetsMDRef;
import eu.scapeproject.model.File;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.IntellectualEntityCollection;
import eu.scapeproject.model.LifecycleState;
import eu.scapeproject.model.LifecycleState.State;
import eu.scapeproject.model.Representation;
import eu.scapeproject.model.VersionList;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.mets.MetsMarshaller;
import eu.scapeproject.model.util.MetsUtil;

public class MockContainer implements Container {

	private static final Logger LOG = LoggerFactory.getLogger(MockContainer.class);
	private final PosixStorage storage;
	private final Map<String, String> metadataIdMap = new HashMap<String, String>();
	private final Map<String, String> fileIdMap = new HashMap<String, String>();
	private final Map<Long, Object> asyncIngestMap = new HashMap<Long, Object>();
	private final AsyncIngester asyncIngester = new AsyncIngester();
	private Thread asyncIngesterThread = new Thread();
	private final int port;

	public MockContainer(String path, int port) {
		this.storage = new PosixStorage(path);
		this.port = port;
	}

	public void start() {
		this.asyncIngesterThread = new Thread(asyncIngester);
		this.asyncIngesterThread.start();
	}

	public void handle(Request req, Response resp) {
		try {
			if (req.getMethod().equals("POST")) {
				handlePost(req, resp);
			} else if (req.getMethod().equals("DELETE")) {

			} else if (req.getMethod().equals("PUT")) {
				handlePut(req, resp);
			} else if (req.getMethod().equals("GET")) {
				handleGet(req, resp);
			} else {
				LOG.error("Unable to handle method of type " + req.getMethod());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void handlePut(Request req, Response resp) {
		String contextPath = req.getPath().getPath();
		LOG.info("-- HTTP/1.1 PUT " + contextPath + " from " + req.getClientAddress().getAddress().getHostAddress());
		try {
			if (contextPath.startsWith("/entity/")) {
				handleUpdate(req, resp);
			} else {
				resp.setCode(404);
				resp.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void handlePost(Request req, Response resp) throws IOException {
		String contextPath = req.getPath().getPath();
		LOG.info("-- HTTP/1.1 POST " + contextPath + " from " + req.getClientAddress().getAddress().getHostAddress());
		try {
			if (contextPath.equals("/entity-async")) {
				handleAsyncIngest(req, resp, 200);
			} else if (contextPath.equals("/entity")) {
				handleIngest(req, resp, 201);
            } else if (contextPath.equals("/entity-list")) {
                handleRetrieveEntityList(req, resp);
			} else {
				resp.setCode(404);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			resp.close();
		}
	}


    private void handleGet(Request req, Response resp) throws IOException {
		String contextPath = req.getPath().getPath();
		LOG.info("-- HTTP/1.1 GET " + contextPath + " from " + req.getClientAddress().getAddress().getHostAddress());
		try {
			if (contextPath.startsWith("/entity/")) {
				handleRetrieveEntity(req, resp);
			} else if (contextPath.startsWith("/metadata/")) {
				handleRetrieveMetadata(req, resp);
			} else if (contextPath.startsWith("/entity-version-list/")) {
				handleRetrieveVersionList(req, resp);
			} else if (contextPath.startsWith("/file/")) {
				handleRetrieveFile(req, resp);
			} else if (contextPath.startsWith("/lifecycle/")) {
				handleRetrieveLifecycleState(req, resp);
			} else {
				resp.setCode(404);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			resp.close();
		}
	}

	private void handleRetrieveLifecycleState(Request req, Response resp) throws Exception{
		String id = req.getPath().getPath().substring(11);
		if (storage.exists(id, 1)) {
			IntellectualEntity entity= MetsMarshaller.getInstance().deserialize(IntellectualEntity.class, new ByteArrayInputStream(storage.getXML(id, 1)));
			MetsMarshaller.getInstance().serialize(entity.getLifecycleState(), resp.getOutputStream());
			return;
		}
		for (Entry<Long, Object> entry:asyncIngestMap.entrySet()){
			if (entry.getValue() instanceof IntellectualEntity){
				IntellectualEntity ent=(IntellectualEntity) entry.getValue();
				if (ent.getIdentifier().getValue().equals(id)){
					MetsMarshaller.getInstance().serialize(ent.getLifecycleState(), resp.getOutputStream());
					return;
				}
			}
		}
		
	}

	private void handleRetrieveFile(Request req, Response resp) throws Exception {
		int version = 1;
		String fileId = req.getPath().getPath().substring(6);
		String entityIdid = fileIdMap.get(fileId);
		boolean found = false;
		if (entityIdid == null) {
			resp.setCode(404);
		} else {
			IntellectualEntity entity = MetsMarshaller.getInstance().deserialize(IntellectualEntity.class,
					new ByteArrayInputStream(storage.getXML(entityIdid, 1)));
			MetsMarshaller.getInstance().serialize(getFile(fileId, entity), resp.getOutputStream());
			resp.setCode(200);
		}
	}

	private File getFile(String id, IntellectualEntity entity) {
		for (Representation r : entity.getRepresentations()) {
			for (File f : r.getFiles()) {
				if (f.getIdentifier().getValue().equals(id)) {
					return f;
				}
			}
		}
		return null;
	}

	private void handleRetrieveVersionList(Request req, Response resp) throws Exception {
		String id = req.getPath().getPath().substring(21);
		List<String> versions = storage.getVersionList(id);
		Collections.sort(versions);
		VersionList versionList=new VersionList(id,versions);
		MetsMarshaller.getInstance().getJaxbMarshaller().marshal(versionList, resp.getOutputStream());
		resp.setCode(200);
	}

	private void handleRetrieveMetadata(Request req, Response resp) throws Exception {
		int version = getVersionFromPath(req.getPath().getPath().substring(10));
		String id = req.getPath().getPath().substring(req.getPath().getPath().lastIndexOf('/') + 1);
		try {
			byte[] blob = storage.getXML(metadataIdMap.get(id), version);
			IntellectualEntity e = MetsMarshaller.getInstance().deserialize(IntellectualEntity.class, new ByteArrayInputStream(blob));
			DCMetadata dc = (DCMetadata) e.getDescriptive();
			resp.set("Content-Type", "text/xml");
			MetsMarshaller.getInstance().getJaxbMarshaller().marshal(MetsUtil.convertDCMetadata(dc), resp.getOutputStream());
			resp.setCode(200);
		} catch (FileNotFoundException e) {
			resp.setCode(404);
		}
	}

	private static int getVersionFromPath(String path) {

		// check if the user requests a specific version
		Matcher m = Pattern.compile("/\\d*/").matcher(path);
		if (m.find()) {
			return Integer.parseInt(path.substring(m.start() + 1, m.end() - 1));
		} else {
			return 1;
		}
	}

	private void handleRetrieveEntity(Request req, Response resp) throws Exception {
		int version = getVersionFromPath(req.getPath().getPath().substring(8));
		String id = req.getPath().getPath().substring(req.getPath().getPath().lastIndexOf('/') + 1);
		try {
			byte[] blob = storage.getXML(id, version);
			if (req.getParameter("useReferences") != null && req.getParameter("useReferences").equalsIgnoreCase("true")) {
				MetsDocument doc = (MetsDocument) MetsMarshaller.getInstance().getJaxbUnmarshaller()
						.unmarshal(new ByteArrayInputStream(blob));
				MetsMDRef.Builder ref = new MetsMDRef.Builder()
						.id(UUID.randomUUID().toString())
						.href("http://localhost:" + port + "/metadata/" + doc.getDmdSec().getId());
				MetsDMDSec dmdSec = new MetsDMDSec.Builder(doc.getDmdSec().getId())
						.mdRef(ref.build())
						.metadataWrapper(null)
						.build();
				MetsDocument.Builder docBuilder = new MetsDocument.Builder(doc)
						.dmdSec(dmdSec);
				MetsMarshaller.getInstance().getJaxbMarshaller().marshal(docBuilder.build(), resp.getOutputStream());
			} else {
				IOUtils.write(blob, resp.getOutputStream());
			}
			resp.setCode(200);
			resp.close();
		} catch (FileNotFoundException e) {
			resp.setCode(404);
		} finally {
			resp.close();
		}
	}

    private void handleRetrieveEntityList(Request req, Response resp) throws Exception {
        String[] uris=req.getContent().split("\\n");
        List<MetsDocument> docs=new ArrayList<MetsDocument>();
        for (String uri:uris){
            byte[] blob=storage.getXML(uri, 1);
            docs.add((MetsDocument) MetsMarshaller.getInstance().getJaxbUnmarshaller().unmarshal(new ByteArrayInputStream(blob)));
        }
        IntellectualEntityCollection entities=new IntellectualEntityCollection(docs);
        MetsMarshaller.getInstance().getJaxbMarshaller().marshal(entities, resp.getOutputStream());
        resp.setCode(200);
    }

    private void handleUpdate(Request req, Response resp) throws Exception {
		try {
			handleIngest(req, resp, 200);
		} finally {
			resp.close();
		}
	}

	private void handleAsyncIngest(Request req, Response resp, int okValue) throws Exception {
		// get entity from request body
		IntellectualEntity.Builder entityBuilder = new IntellectualEntity.Builder(MetsMarshaller.getInstance().deserialize(
				IntellectualEntity.class, req.getInputStream()));
		entityBuilder.lifecycleState(new LifecycleState("async ingest", State.INGESTING));
		IntellectualEntity entity = entityBuilder.build();

		// add the entity to the async map, for later ingestion,
		// and set the the time the ingestion should be processed by the mock
		long time = new Date().getTime();
		do {
			time += new Random().nextInt(5000) + 5000;
		}while (asyncIngestMap.containsKey(time));
		asyncIngestMap.put(time, entity);

		// have to check for id existence and generate some if necessary
		if (entity.getIdentifier() == null) {
			entityBuilder.identifier(new Identifier(UUID.randomUUID().toString()));
		}

		// return the identity to get the lifecyclestate
		MetsMarshaller.getInstance().serialize(entity.getIdentifier(), resp.getOutputStream());
		resp.setCode(okValue);
	}

	private void handleIngest(Request req, Response resp, int okValue) throws Exception {
		try {
			IntellectualEntity.Builder entityBuilder = new IntellectualEntity.Builder(MetsMarshaller.getInstance().deserialize(
					IntellectualEntity.class, req.getInputStream()));
			entityBuilder.lifecycleState(new LifecycleState("", State.INGESTED));
			IntellectualEntity entity = entityBuilder.build();
			
			ingestEntity(entity);

			// generate the server response with the ingested entity's id
			resp.setCode(okValue);
			resp.set("Content-Type", "text/plain");
			IOUtils.copy(new ByteArrayInputStream(entity.getIdentifier().getValue().getBytes()), resp.getOutputStream());
		} catch (IOException e) {
			resp.setCode(500);
			throw new SerializationException(e);
		}
	}

	public void ingestObject(Object value) throws Exception {
		if (value instanceof IntellectualEntity) {
			IntellectualEntity entity=(IntellectualEntity) value;
			ingestEntity(entity);
		}
	}

	private void ingestEntity(IntellectualEntity entity) throws Exception{
		IntellectualEntity.Builder entityBuilder=new IntellectualEntity.Builder(entity);
		// have to check for id existence and generate some if necessary
		if (entity.getIdentifier() == null) {
			entityBuilder.identifier(new Identifier(UUID.randomUUID().toString()));
		}
		if (entity.getDescriptive() == null || entity.getDescriptive().getId() == null) {
			DCMetadata.Builder dc = new DCMetadata.Builder((DCMetadata) entity.getDescriptive())
					.identifier(new Identifier(UUID.randomUUID().toString()));
			entityBuilder.descriptive(dc.build());
		}
		// add the lifeccle state
		entityBuilder.lifecycleState(new LifecycleState("ingested", State.INGESTED));
		// now build the entity again with proper identifiers
		entity = entityBuilder.build();
		LOG.debug("writing entity " + entity.getIdentifier().getValue());

		// save the file identifiers to the according map
		for (Representation r : entity.getRepresentations()) {
			for (File f : r.getFiles()) {
				fileIdMap.put(f.getIdentifier().getValue(), entity.getIdentifier().getValue());
			}
		}

		// save the data in the storage backend
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		MetsMarshaller.getInstance().serialize(entity, bos);
		int version = entity.getVersionNumber();
		if (storage.exists(entity.getIdentifier().getValue(), entity.getVersionNumber())) {
			version = storage.getNewVersionNumber(entity.getIdentifier().getValue());
		}
		storage.saveXML(bos.toByteArray(), entity.getIdentifier().getValue(), version, false);
		LOG.debug("wrote new file " + entity.getIdentifier().getValue());

		// update the hashmap with the metadata references to the entities
		metadataIdMap.put(entity.getDescriptive().getId(), entity.getIdentifier().getValue());
	}

	public void close() throws Exception {
		this.asyncIngester.stop();
		this.asyncIngesterThread.join();
		this.purgeStorage();
	}

	public void purgeStorage() throws Exception {
		storage.purge();
	}

	public class AsyncIngester implements Runnable {
		private boolean stop = false;
		
       

		public synchronized void stop() {
			stop = true;
		}

		public void run() {
			while (!stop) {
				for (Entry<Long, Object> asyncRequest : asyncIngestMap.entrySet()) {
					if (new Date().getTime() >= asyncRequest.getKey()) {
						try {
							LOG.info("ingesting object at " + asyncRequest.getKey());
							ingestObject(asyncRequest.getValue());
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							asyncIngestMap.remove(asyncRequest.getKey());
						}
					}
				}
				try{
					Thread.sleep(500);
				}catch(InterruptedException e){
					e.printStackTrace();
					stop=true;
				}
			}
		};
	}

}
