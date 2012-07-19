package eu.scapeproject;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.simpleframework.transport.connect.SocketConnection;

public class ConnectorAPIMock implements Runnable{

    private final int port = 8783;
    private final EntityContainer container=new EntityContainer();
    
    private SocketConnection conn;
    private volatile boolean running=false;
    
    public int getPort(){
        return this.port;
    }
    
    public void run(){
        this.running=true;
        try {
            this.conn=new SocketConnection(this.container);
            this.startServer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void stop() throws IOException{
        if (this.conn == null){
            throw new IOException("Connection is null");
        }
        this.conn.close();
        this.running=false;
    }
    
    private void startServer() throws IOException{
        EntityContainer container=new EntityContainer();
        this.conn=new SocketConnection(container);
        this.conn.connect(new InetSocketAddress(this.port));
    }
    
    public synchronized boolean isRunning() {
        return this.running;
    }
}
