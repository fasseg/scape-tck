package eu.scapeproject;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityContainer implements Container {
	private static final Logger LOG = LoggerFactory.getLogger(EntityContainer.class);
	private final PosixStorage storage;

	public EntityContainer(String path) {
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
			handleIngest(req, resp);
			if (contextPath.equals("/entity")) {
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
			if (contextPath.substring(0, 7).equals("/entity")) {
				handleRetrieve(req, resp);
			} else {
				resp.setCode(404);
				resp.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void handleRetrieve(Request req, Response resp) throws Exception {
		String id = req.getPath().getPath().substring(8);
		try{
			byte[] blob = storage.getXML(id);
			IOUtils.write(blob, resp.getOutputStream());
			resp.setCode(200);
			resp.close();
		}catch (FileNotFoundException e){
			resp.setCode(404);
			resp.close();
		}
	}

	private void handleIngest(Request req, Response resp) throws Exception {
		String xml = req.getContent();
		int objIdPos = xml.indexOf("OBJID=\"") + 7;
		int nextQuotePos = xml.indexOf("\"", objIdPos);
		String id = xml.substring(objIdPos, nextQuotePos);
		storage.saveXML(xml.getBytes(), id, false);
		LOG.debug("wrote new file " + id + " with " + xml.length() + " bytes");
		resp.setCode(201);
		resp.close();
	}

	public void purgeStorage() throws Exception {
		storage.purge();
	}
}
