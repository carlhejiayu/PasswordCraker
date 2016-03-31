import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by herbert on 2016-03-26.
 */
public class Worker extends Thread{
    static CountDownLatch nodeCreateSignal = new CountDownLatch(1);
    ZooKeeperConnector zooKeeperConnector;
    String workerGroupPath = "/workersGroup";
    String myPath = "/worker";
    String taskQueue = "taskWaitingQueue";
    String processQueue = "taskProcessQueue";
    IpAddress fileServerAddress;
    AtomicBoolean fileServerOk;
    String zookeeperHost;
    ZookeeperQueue taskwaitingqueue;
    ZookeeperQueue taskProcessQueue;
    String myActualPath;


    public static void main(String[] args) {
        String zkHost = args[0];
        Worker worker = new Worker(zkHost);
        worker.checkpath();
        worker.createSelfNode();
        worker.getFileServerAddress();
        worker.start();

    }

    @Override
    public void run() {
        while (true){
            if(fileServerOk.get()){
                getTask();
            }
        }
    }


    public Worker(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
        zooKeeperConnector = new ZooKeeperConnector();
        //connect to zookeeper
        try {
            zooKeeperConnector.connect(zookeeperHost);

        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        fileServerOk = new AtomicBoolean(false);
        System.out.println(zooKeeperConnector.getZooKeeper());
        taskwaitingqueue = new ZookeeperQueue(taskQueue, zooKeeperConnector);
        //taskwaitingqueue.tryCreate();
        taskProcessQueue = new ZookeeperQueue(processQueue, zooKeeperConnector);
        //taskProcessQueue.tryCreate();
    }
    public void doTask(String hashword, List<String> dictionary){
        System.out.println("start doing task:" + hashword);
        for(String word : dictionary){
            String hash = MD5Hash.getHash(word);
            if(hash.equals(hashword)){
                //success
                report(word, hashword, true);
                return;
            }
        }
        report("notFound", hashword, false);
    }

    public void report(String answer, String task, boolean success){
        ZookeeperQueue reportQueue = null;
        System.out.println("start to report:" + answer + " for " + task);
        if (success) {
            reportQueue = new ZookeeperQueue("jobs/" + task + "/success", zooKeeperConnector);
        }
        else {
            reportQueue = new ZookeeperQueue("jobs/" + task + "/notFound", zooKeeperConnector);
        }
        try {
            reportQueue.insert(answer);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void getTask(){
        String task = taskwaitingqueue.pop();
        String processPath = null;
        try {
            processPath = taskProcessQueue.insert(myActualPath + "=" + task);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String[] tasks = task.split("-");
        String hashword = tasks[0];
        String partitionId = tasks[1];
        List dict = getFileFromFileServer(partitionId);
        doTask(hashword, dict);
        taskProcessQueue.deletePath(processPath);
    }

    public List getFileFromFileServer(String partition){
        try {
            Socket socket = new Socket(fileServerAddress.Ip, fileServerAddress.port);
            ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeBytes(partition + "\r\n");
            return (List) input.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println("Error: didn't get the dictionary");
        return null;
    }

    public void createSelfNode(){
        String actualpath = zooKeeperConnector.createReturnPath(
                workerGroupPath + myPath,         // Path of znode
                null,           // Data not needed.
                CreateMode.EPHEMERAL_SEQUENTIAL   // Znode type, set to EPHEMERAL SEQUENTIAL.
        );

        System.out.println("create the worker! : " + actualpath);
        myActualPath = actualpath;

    }
    private void checkpath() {
        Stat stat = zooKeeperConnector.exists(workerGroupPath, null);

        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating new workersGroup at: " + workerGroupPath);
            KeeperException.Code ret = zooKeeperConnector.create(
                    workerGroupPath,         // Path of znode
                    null,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("create the workersGroup!");
        }
    }

    private void getFileServerAddress(){
        Stat stat = zooKeeperConnector.exists("/fileServer", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //handle file server failure
                String path = event.getPath();
                Event.EventType type = event.getType();
                if(type == Event.EventType.NodeDeleted ){
                    System.out.println("file server crash, waiting for new file server");
                    fileServerOk.set(false);
                    getFileServerAddress();
                }
                if(type == Event.EventType.NodeCreated){
                    //now backup file server up, get new address
                    System.out.println("new file server up");
                    getFileServerAddress();
                }
            }
        });
        if(stat != null){
            stat = null;
            try {
                byte[] b = zooKeeperConnector.getZooKeeper().getData("/fileServer", null, stat);
                //ByteBuffer buffer = ByteBuffer.wrap(b);
                String address = new String(b);
                System.out.println("address is: " + address);
                IpAddress ipAddress = IpAddress.parseAddressString(address);
                fileServerAddress = ipAddress;
                fileServerOk.set(true);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
