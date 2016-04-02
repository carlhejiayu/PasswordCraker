import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by herbert on 2016-03-29.
 */
public class ClientDriver {
    ZooKeeperConnector zooKeeperConnector;
    AtomicBoolean jobTrackerOk;
    IpAddress jobTrackerAddress;
    BufferedReader input;
    DataOutputStream output;
    Socket socket;
    String selfname;
    public ClientDriver(String zookeeperHost) {
        jobTrackerOk = new AtomicBoolean(false);
        zooKeeperConnector = new ZooKeeperConnector();
        //connect to zookeeper
        try {
            zooKeeperConnector.connect(zookeeperHost);
        } catch(Exception e) {
            System.out.println("Zookeeper connect error: "+ e.getMessage());
        }

    }

    public static void main(String[] args) {
        String zkHost = args[0];
        String type = args[1];
        String parameter = args[2];

        ClientDriver client = new ClientDriver(zkHost);
        client.getJobTrackerAddress();

        if(type.equals("job")){
          String password = parameter;

          while(client.jobTrackerOk.get() == false){
            //wait for it to be reconnect
          }
          client.sendJob(password);

        }
        else if(type.equals("status")){
            while(client.jobTrackerOk.get() == false){
                //wait for it to be reconnect
            }
            String status = client.checkStatus(parameter);
            System.out.println(status);

        }
    }
    public String checkStatus(String request){
        try {
            output.writeBytes("status-" + request + "\r\n");
            String answer = input.readLine();
            return answer;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;

    }
    public void sendJob(String password){
        try {

            output.writeBytes("job-" + password + "\r\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //todo: reconnect to the job tracker B send with my name
    public void getJobTrackerAddress(){
        Stat stat = zooKeeperConnector.exists("/JobTracker", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //handle file server failure
                String path = event.getPath();
                Event.EventType type = event.getType();
                if(type == Event.EventType.NodeDeleted ){
                    //System.out.println("job tracker server crash, waiting for new file server");
                    jobTrackerOk.set(false);
                    getJobTrackerAddress();
                }
                if(type == Event.EventType.NodeCreated){
                    //now backup file server up, get new address
                    //System.out.println("new job tracker server up");
                    getJobTrackerAddress();
                }
            }
        });
        if(stat != null){
            stat = null;
            try {
                byte[] b = zooKeeperConnector.getZooKeeper().getData("/JobTracker", null, stat);
                //ByteBuffer buffer = ByteBuffer.wrap(b);

                String address = new String(b);

                //System.out.println("address is: " + address);
                IpAddress ipAddress = IpAddress.parseAddressString(address);
                jobTrackerAddress = ipAddress;
                socket = new Socket(jobTrackerAddress.Ip, jobTrackerAddress.port);
                input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                output = new DataOutputStream(socket.getOutputStream());
                //selfname = zooKeeperConnector.createReturnPath("/clients/client", null, CreateMode.EPHEMERAL_SEQUENTIAL);
                jobTrackerOk.set(true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
