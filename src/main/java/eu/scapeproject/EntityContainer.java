package eu.scapeproject;

import java.io.IOException;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityContainer implements  Container {
    private static final Logger LOG = LoggerFactory.getLogger(EntityContainer.class);

    public void handle(Request req, Response resp) {
        if (req.getMethod().equals("POST")) {
            handlePost(req,resp);
        } else if (req.getMethod().equals("DELETE")){
            
        } else if (req.getMethod().equals("PUT")){
            
        } else if (req.getMethod().equals("GET")){
            
        } else {
            LOG.error("Unable to handle method of type " + req.getMethod());
        }
    }

    private void handlePost(Request req, Response resp) {
        String contextPath=req.getPath().getPath();
        LOG.info("-- HTTP/1.1 POST " + contextPath);
        if (contextPath.equals("/entity")){
            try {
                handleIngest(req,resp);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleIngest(Request req, Response resp) throws IOException {
        System.out.println(req.getContent());
        resp.setCode(201);
        resp.close();
    }
}
