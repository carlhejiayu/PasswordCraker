

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jiayuhe on 2016-03-26.
 */
public class JobTracker {
    String root = "/JobTracker";
    String jobsRoot = "/jobs";
    ZooKeeperConnector zkc;
    Watcher watcher;
    ZookeeperQueue taskWaitingQueue;
    ZookeeperQueue taskProcessingQueue;
    ServerSocket serverSocket;
    String selfAddress;
    int selfport;


    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Test zkServer:clientPort selfport");
            return;
        }

        JobTracker t = new JobTracker(args[0], Integer.parseInt(args[1]));

        t.checkpath();

        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }

    public JobTracker(String hosts,int selfport) {
        zkc = new ZooKeeperConnector();
        this.selfport = selfport;
        try {
            this.selfAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        taskWaitingQueue = new ZookeeperQueue("taskWaitingQueue", zkc) ;
        taskWaitingQueue.tryCreate();
        taskProcessingQueue = new  ZookeeperQueue("taskProcessingQueue", zkc);
        taskProcessingQueue.tryCreate();
        try {
            serverSocket = new ServerSocket(selfport);
        } catch (IOException e) {
            e.printStackTrace();
        }


        watcher = new Watcher() { // Anonymous Watcher for the JobTracker Primary Purpose
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } };

    }

    private void checkpath() {
        Stat stat = zkc.exists(root, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + root);
            KeeperException.Code ret = zkc.create(
                    root,         // Path of znode
                    selfAddress+":"+selfport,           // Data not needed.
                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("the boss!");


            // after becoming the boss,then, we could let it perform the jobRequest function
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try {
                            new jobRequestHandlingThread(serverSocket.accept(), zkc, taskWaitingQueue,  taskProcessingQueue).start();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }

        //this one will only be created once
        Stat jobrootsta = zkc.exists(jobsRoot, watcher);
        if (jobrootsta == null){
            System.out.println("Creating " + jobsRoot);
            KeeperException.Code ret = zkc.create(
                    jobsRoot,         // Path of znode
                    null,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("successfully create the root for Jobs!");
        }

    }



    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        Watcher.Event.EventType type = event.getType();
        if(path.equalsIgnoreCase(root)) {
            if (type == Watcher.Event.EventType.NodeDeleted) {
                System.out.println(root + " deleted! Let's go!");
                checkpath(); // try to become the boss
            }
            if (type == Watcher.Event.EventType.NodeCreated) {
                System.out.println(root + " created!");
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }
}

class jobRequestHandlingThread extends Thread{
    Socket requestSocket;
    ZooKeeperConnector zkc;
    Watcher successwatcher;
    Watcher failwatcher;
    Watcher workerwatcher;
    ObjectOutputStream objectOutputStream;
    String ClientName;

    ZookeeperQueue taskWaitingQueue;
    ZookeeperQueue taskProcessingQueue;

    public jobRequestHandlingThread(Socket requestSocket,ZooKeeperConnector zkc,ZookeeperQueue taskWaitingQueue, ZookeeperQueue taskProcessingQueue) {
        this.requestSocket = requestSocket;
        this.zkc = zkc;
        this.taskProcessingQueue =taskProcessingQueue;
        this.taskWaitingQueue = taskWaitingQueue;

        System.out.println("Construct a jobRequestHandlingThread");
        successwatcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                successTaskHandleEvent(event);
            } };

        failwatcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                failTasksHandleEvent(event);
            }
        };

        workerwatcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                workerWatcherHandler(event);
            }
        };
        try{
            zkc.getZooKeeper().getChildren("/workersGroup",workerwatcher);
        }
        catch(Exception e){
            System.out.println("Cannot set watch on the worker thread");
        }



    }

    private void checkJobState(){
        //we need to check whether some of the jobs have been finished during the time when it is crashed and restablish a new primary job Tracker
        try {
            List <String> alljobs = zkc.getZooKeeper().getChildren("/jobs",null);
            for (String jobpath: alljobs){
                String jobinfo = new String(zkc.getZooKeeper().getData(jobpath,null,null));
                String [] jobi = jobinfo.split("-");
                int Task_Number = Integer.parseInt(jobi[0]);
                String client  = jobi[1];
                int notFoundNumber = zkc.getZooKeeper().getChildren(jobpath+"/notFound", null).size();
                int successNumber  = zkc.getZooKeeper().getChildren(jobpath+"/success", null).size();
                if (Task_Number == notFoundNumber + successNumber && client.equals(ClientName)){
                    //Now we can respond to the right client
                    String message = "";
                    if (successNumber == 1){
                        String password = new String (zkc.getZooKeeper().getData(jobpath+"/success",null,null));
                        message = "Job finished & the password:" + password+"\r\n";
                    }
                    else{
                        message = "Job finished & Password Not Found \r\n";
                    }
                    objectOutputStream.writeObject(message);
                }


            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void splitJobs(String word, int worker_number){
        try {
            for (int i = 0; i<worker_number; i++) {
                String info = "";
                info = word + "-" + String.valueOf(i+1)+":"+String.valueOf(worker_number);
                taskWaitingQueue.insert(info);
            }

        }
        catch(Exception e){
            e.printStackTrace();
        }
    }


    public String slice_end(String s, int endIndex) {
        if (endIndex < 0) endIndex = s.length() + endIndex;
        return s.substring(0, endIndex);
    }


    private void failTasksHandleEvent(WatchedEvent event){
        String path = event.getPath();
        String pattern = "/notFound";
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(path);
        int startindex = 1;
        if (m.find( )) {
            startindex = m.start();
        }
        //only obtain the path of that specific job
        String jobpath = slice_end(path, startindex -1);
        Watcher.Event.EventType type = event.getType();
        if (type == Watcher.Event.EventType.NodeCreated){
            try {
                //now we look for the the number of notFound case
                String jobdata = new String (zkc.getZooKeeper().getData(jobpath, null, null));
                String []jdata = jobdata.split("-");
                int Task_number = Integer.parseInt(jdata[0]);
                String client = jdata[1];
                int notFoundNumber = zkc.getZooKeeper().getChildren(path, null).size();

                // now we need to look for the number of success case
                int successNumber = zkc.getZooKeeper().getChildren(jobpath+"/success",null, null).size();

                if (notFoundNumber + successNumber == Task_number){
                    //that mean, we are already collect all Tasks for this jobs and we will be able to delete
                    if (successNumber == 0){
                        objectOutputStream.writeObject("Job finished & the password is not Found \r\n");
                    }
                    //we can delete the job node


                }
                else{
                    //reset watcher when the jobs are not finished
                    try {
                        zkc.getZooKeeper().getChildren("/workersGroup", workerwatcher);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        else {
            // reset watcher if something bad happens
            try {
                zkc.getZooKeeper().getChildren("/workersGroup", workerwatcher);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private  void successTaskHandleEvent(WatchedEvent event){
        /*Need to check whether the total number of succeess and failure is equal to the job info*/
        String path = event.getPath();
        String pattern = "/success";
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(path);
        int startindex = 1;
        if (m.find( )) {
            startindex = m.start();
        }
        //only obtain the path of that specific job
        String jobpath = slice_end(path, startindex -1);
        Watcher.Event.EventType type = event.getType();
        if (type == Watcher.Event.EventType.NodeCreated){
            try {
                String jobdata = new String (zkc.getZooKeeper().getData(jobpath, null, null));
                String []jdata = jobdata.split("-");
                int Task_number = Integer.parseInt(jdata[0]);
                String client = jdata[1];
                int notFoundNumber = zkc.getZooKeeper().getChildren(jobpath+"/notFound", null).size();
                if (notFoundNumber + 1 == Task_number){
                    //that mean, we are already collect all Tasks for this jobs and we will be able to delete
                    //we can delete the job node
                    String password = new String (zkc.getZooKeeper().getData(path,null,null));
                    //aslo we can send the back the successful message to
                    objectOutputStream.writeObject("Job finished & the password:" + password+"\r\n");
                }
                else{
                    //reset watchwer because the jobs are not finshed yet
                    try {
                        zkc.getZooKeeper().getChildren(jobpath, workerwatcher);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
        else {
            // reset watcher if something else happen
            try {
                zkc.getZooKeeper().getChildren("/workersGroup", workerwatcher);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // need to send back to clients for Found

    }
    private  void workerWatcherHandler (WatchedEvent event){
        // When woker fail
        String failpath = event.getPath();
        Watcher.Event.EventType type = event.getType();
        if (type == Watcher.Event.EventType.NodeDeleted){
            //One worker fails
            try {
                List<String> allProcessingTasks = zkc.getZooKeeper().getChildren("/taskProcessingQueue", null);
                for (String eachpath: allProcessingTasks){
                    String[] s =  eachpath.split("=");
                    String workerpath  = s[0];
                    if (workerpath == failpath){
                        taskProcessingQueue.deletePath(workerpath);
                        taskWaitingQueue.insert(s[1]);
                    }

                }
            }
            catch (Exception e){
                System.out.println("Cannot all the tasks");
            }
        }
        //reset watcher
        try{
            zkc.getZooKeeper().getChildren("/workersGroup",workerwatcher);}
        catch (Exception e){
            e.printStackTrace();
        }


    }


    @Override
    public void run() {
        System.out.println("Begin to Run");
        while (true){
            try {
                objectOutputStream = new ObjectOutputStream(requestSocket.getOutputStream());
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(requestSocket.getInputStream()));
                String request = bufferedReader.readLine();
                System.out.println("I receiving "+request);

                String r[] = request.split("-");
                String requestword = r[0];
                String client = r[1];


                if (requestword.equals( "connect")) {
                    ClientName = client;
                    checkJobState();
                } else {
                    //Now we need to create a node for a job that request to crack the word , 'requestword'
                    String path = "/jobs/" + requestword;
                    //Stat stat = zkc.exists(path, jobswatcher);


                    // get the number of worker
                    List workers = zkc.getZooKeeper().getChildren("/workersGroup", true);
                    int worker_number = workers.size();
                    String jobInfo = String.valueOf(worker_number) + "-" + client;
                    //if (stat == null) {              // znode doesn't exist; let's try creating it
                    System.out.println("Creating the job of the word: " + requestword);
                    KeeperException.Code ret = zkc.create(
                            path,         // Path of znode
                            jobInfo,           // Data not needed.
                            CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                    );
                    // }
                    String successpath = path + "/success";
                    String failpath = path + "/notFound";
                    zkc.create(successpath, null, CreateMode.PERSISTENT);
                    zkc.create(failpath, null, CreateMode.PERSISTENT);
                    zkc.getZooKeeper().getChildren(successpath, successwatcher);
                    zkc.getZooKeeper().getChildren(failpath, failwatcher);

                    // Now we need to split the jobs and push the tasks into the Queue
                    splitJobs(requestword, worker_number);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println("Not succesffully get the number of worker");
            }

        }
    }
}
