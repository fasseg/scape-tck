package eu.scapeproject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;

import org.simpleframework.transport.connect.SocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock class for API testing
 * 
 * @author Frank Asseg
 * 
 */
public class ConnectorAPIMock implements Runnable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ConnectorAPIMock.class);
	private final int port = 8783;
	private final String path;
	private SocketConnection conn;
	private volatile boolean running = false;
	private MockContainer container;
	private long startupMem;

	public ConnectorAPIMock() {
		this.path = System.getProperty("java.io.tmpdir") + "/scape-tck-" + System.getProperty("user.name");
	}

	public int getPort() {
		return this.port;
	}

	public void run() {
		try {
			this.startServer();
			this.running = true;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void stop() throws IOException {
		DecimalFormat fmt = new DecimalFormat("#.##");
		if (this.conn == null) {
			throw new IOException("Connection is null");
		}
		this.conn.close();
		this.running = false;
		LOG.debug(">> total used:\t" + fmt.format((double) Runtime.getRuntime().totalMemory() / (1024d * 1024d)) + " MB ");
		LOG.debug(">> after start:\t" + fmt.format(startupMem / (1024d * 1024d)) + " MB");
		LOG.debug(">> growth:\t\t" + fmt.format((Runtime.getRuntime().totalMemory() - startupMem)/(1024d*1024d)) + " MB");
	}

	private void startServer() throws IOException {
		this.container = new MockContainer(this.path, this.port);
		this.conn = new SocketConnection(this.container);
		this.startupMem = Runtime.getRuntime().totalMemory();
		this.container.start();
		this.conn.connect(new InetSocketAddress(this.port));
	}

	public synchronized boolean isRunning() {
		return this.running;
	}

	public void close() throws Exception {
		this.container.close();
	}
}
