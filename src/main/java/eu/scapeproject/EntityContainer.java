package eu.scapeproject;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityContainer implements Container {
	private static final Logger LOG = LoggerFactory.getLogger(EntityContainer.class);
	private final PosixStorage storage;
	
	public EntityContainer(String path) {
		this.storage=new PosixStorage(path);
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
		if (contextPath.equals("/entity")) {
			try {
				handleIngest(req, resp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void handleGet(Request req,Response resp){
		String contextPath = req.getPath().getPath();
		LOG.info("-- HTTP/1.1 GET " + contextPath);
		if (contextPath.substring(0,7).equals("/entity")) {
			try {
				handleRetrieve(req, resp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void handleRetrieve(Request req, Response resp) throws Exception{
		String id = req.getPath().getPath().substring(8);
		byte[] blob = storage.getFOXML(id);
		IOUtils.write(blob, resp.getOutputStream());
		resp.setCode(200);
		resp.close();
	}

	private void handleIngest(Request req, Response resp) throws Exception {
		String foxml = req.getContent();
		int objIdPos = foxml.indexOf("OBJID=\"") + 7;
		int nextQuotePos = foxml.indexOf("\"", objIdPos);
		String id = foxml.substring(objIdPos, nextQuotePos);
		storage.saveFOXML(foxml.getBytes(), id, false);
		LOG.debug("wrote new file " + id + " with " + foxml.length() + " bytes");
		resp.setCode(201);
		resp.close();
	}
	
	public void purgeStorage() throws Exception {
		storage.purge();
	}
}
