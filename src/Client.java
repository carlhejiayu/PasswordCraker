import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by herbert on 2016-03-29.
 */
public class Client {
    ZooKeeperConnector zooKeeperConnector;
    AtomicBoolean jobTrackerOk;
    IpAddress jobTrackerAddress;
    public Client(String zookeeperHost) {
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
        Client client = new Client(zkHost);
        client.getJobTrackerAddress();
        Scanner scanner = new Scanner(System.in);
        while(true) {
            System.out.println("please enter the hashed password");


            String password = scanner.nextLine();
            while(client.jobTrackerOk.get() == false){
                //wait for it to be reconnect
            }
            String answer = client.sendJob(password);

            System.out.println("The answer is: " + answer);
        }

    }

    public String sendJob(String password){
        try {
            Socket socket = new Socket(jobTrackerAddress.Ip, jobTrackerAddress.port);
            BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeBytes(password + "/r/n");
            String answer = input.readLine();
            return answer;
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Error: didn't get the response from job tracker");
        return null;
    }

    public void getJobTrackerAddress(){
        Stat stat = zooKeeperConnector.exists("/JobTracker", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //handle file server failure
                String path = event.getPath();
                Event.EventType type = event.getType();
                if(type == Event.EventType.NodeDeleted ){
                    System.out.println("job tracker server crash, waiting for new file server");
                    jobTrackerOk.set(false);
                    getJobTrackerAddress();
                }
                if(type == Event.EventType.NodeCreated){
                    //now backup file server up, get new address
                    System.out.println("new job tracker server up");
                    getJobTrackerAddress();
                }
            }
        });
        if(stat != null){
            stat = null;
            try {
                byte[] b = zooKeeperConnector.getZooKeeper().getData("/JobTracker", null, stat);
                //ByteBuffer buffer = ByteBuffer.wrap(b);
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(b);
                String address = stringBuilder.toString();
                System.out.println("address is: " + address);
                IpAddress ipAddress = IpAddress.parseAddressString(address);
                jobTrackerAddress = ipAddress;
                jobTrackerOk.set(true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
