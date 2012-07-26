package eu.scapeproject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationException;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.dto.mets.MetsDocument;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.LifecycleState;
import eu.scapeproject.model.LifecycleState.State;
import eu.scapeproject.model.metadata.dc.DCMetadata;
import eu.scapeproject.model.mets.MetsMarshaller;
import eu.scapeproject.model.util.MetsUtil;

public class MockContainer implements Container {
	private static final Logger LOG = LoggerFactory.getLogger(MockContainer.class);
	private final PosixStorage storage;
	private final Map<String, String> metadataIdMap = new HashMap<String, String>();

	public MockContainer(String path) {
		this.storage = new PosixStorage(path);
	}

	public void handle(Request req, Response resp) {
		if (req.getMethod().equals("POST")) {
			handlePost(req, resp);
		} else if (req.getMethod().equals("DELETE")) {

		} else if (req.getMethod().equals("PUT")) {

		} else if (req.getMethod().equals("GET")) {
			handleGet(req, resp);
		} else {
			LOG.error("Unable to handle method of type " + req.getMethod());
		}
	}

	private void handlePost(Request req, Response resp) {
		String contextPath = req.getPath().getPath();
		LOG.info("-- HTTP/1.1 POST " + contextPath);
		try {
			if (contextPath.equals("/entity")) {
				handleIngest(req, resp);
			} else {
				resp.setCode(404);
				resp.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void handleGet(Request req, Response resp) {
		String contextPath = req.getPath().getPath();
		LOG.info("-- HTTP/1.1 GET " + contextPath);
		try {
			if (contextPath.startsWith("/entity/")) {
				handleRetrieve(req, resp);
			} else if (contextPath.startsWith("/metadata/")) {
				handleRetrieveMetadata(req, resp);
			} else {
				resp.setCode(404);
				resp.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void handleRetrieveMetadata(Request req, Response resp) throws Exception {
		String id = req.getPath().getPath().substring(10);
		try {
			byte[] blob = storage.getXML(metadataIdMap.get(id));
			IntellectualEntity e = MetsMarshaller.getInstance().deserialize(IntellectualEntity.class, new ByteArrayInputStream(blob));
			DCMetadata dc = (DCMetadata) e.getDescriptive();
			resp.set("Content-Type", "text/xml");
			MetsMarshaller.getInstance().getJaxbMarshaller().marshal(MetsUtil.convertDCMetadata(dc), resp.getOutputStream());
			resp.setCode(200);
		} catch (FileNotFoundException e) {
			resp.setCode(404);
		} finally {
			resp.close();
		}
	}

	private void handleRetrieve(Request req, Response resp) throws Exception {
		String id = req.getPath().getPath().substring(8);
		try {
			byte[] blob = storage.getXML(id);
			IOUtils.write(blob, resp.getOutputStream());
			resp.setCode(200);
			resp.close();
		} catch (FileNotFoundException e) {
			resp.setCode(404);
		} finally {
			resp.close();
		}
	}

	private void handleIngest(Request req, Response resp) throws Exception {
		try {
			IntellectualEntity.Builder entityBuilder = new IntellectualEntity.Builder(MetsMarshaller.getInstance().deserialize(
					IntellectualEntity.class, req.getInputStream()));
			entityBuilder.lifecycleState(new LifecycleState("", State.INGESTED));
			IntellectualEntity entity = entityBuilder.build();

			// have to check for id existence and generate some if necessary
			if (entity.getIdentifier() == null) {
				entityBuilder.identifier(new Identifier(UUID.randomUUID().toString()));
			}
			if (entity.getDescriptive() == null || entity.getDescriptive().getId() == null) {
				DCMetadata.Builder dc = new DCMetadata.Builder((DCMetadata) entity.getDescriptive())
						.identifier(new Identifier(UUID.randomUUID().toString()));
				entityBuilder.descriptive(dc.build());
			}
			// now build the entity again with proper identifiers
			entity = entityBuilder.build();
			LOG.debug("writing entity " + entity.getIdentifier().getValue());

			// save the data in the storage backend
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			MetsMarshaller.getInstance().serialize(entity, bos);
			storage.saveXML(bos.toByteArray(), entity.getIdentifier().getValue(), false);
			LOG.debug("wrote new file " + entity.getIdentifier().getValue());

			// update the hashmap with the metadata references to the entities
			metadataIdMap.put(entity.getDescriptive().getId(), entity.getIdentifier().getValue());

			// generate the server response with the ingested entity's id
			resp.setCode(201);
			resp.set("Content-Type", "text/plain");
			IOUtils.copy(new ByteArrayInputStream(entity.getIdentifier().getValue().getBytes()), resp.getOutputStream());
		} catch (IOException e) {
			resp.setCode(500);
			throw new SerializationException(e);
		} finally {
			resp.close();

		}
	}

	public void purgeStorage() throws Exception {
		storage.purge();
	}
}
