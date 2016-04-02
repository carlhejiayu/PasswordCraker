

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
    Watcher workerwatcher;
    Watcher workerGroupWatch;
    ZookeeperQueue taskWaitingQueue;
    ZookeeperQueue taskProcessingQueue;
    ZookeeperQueue jtSequence;
    ServerSocket serverSocket;
    String selfAddress;
    int selfport;


    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Test zkServer:clientPort selfport");
            return;
        }

        JobTracker t = new JobTracker(args[0], 7500);

        t.checkpath();

        System.out.println("Sleeping...");
        while (true) {
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
            }
        }
    }

    public JobTracker(String hosts, int selfport) {
        zkc = new ZooKeeperConnector();
        try {
            this.selfAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            zkc.connect(hosts);
        } catch (Exception e) {
            System.out.println("Zookeeper connect " + e.getMessage());
        }

        //job Tracker sequence Queue
        jtSequence = new ZookeeperQueue("jtSequencer", zkc);
        jtSequence.tryCreate();


        taskWaitingQueue = new ZookeeperQueue("taskWaitingQueue", zkc);
        taskWaitingQueue.tryCreate();
        taskProcessingQueue = new ZookeeperQueue("taskProcessQueue", zkc);
        taskProcessingQueue.tryCreate();


        //selfport
        int inc_port = jtSequence.insertAndGetSequence();
        this.selfport = selfport + inc_port ;



        try {
            serverSocket = new ServerSocket(this.selfport);
        } catch (IOException e) {
            e.printStackTrace();
        }

        watcher = new Watcher() { // Anonymous Watcher for the JobTracker Primary Purpose
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            }
        };

    }
    private void checkUnfinishedSplitJobs(){
        //Check-jobs info
        try {
            List<String> alljobs = zkc.getZooKeeper().getChildren("/jobs",null);
            for (String eachjob: alljobs){
                String jobPath = "/jobs/"+ eachjob;
                String jobinfo = new String (zkc.getZooKeeper().getData(jobPath,null,null));
                String []jobi = jobinfo.split("-");
                int Task_Number = Integer.parseInt(jobi[0]);
                int Current_Number = Integer.parseInt(jobi[1]);
                if (Task_Number != Current_Number){
                    //that means the JobTracker fail during the process of splitting Task
                    //we need to continue adding the Tasks
                    for (int i = Current_Number; i <Task_Number; i ++ ){
                        String info = eachjob + "-" + String.valueOf(i + 1) + ":" + String.valueOf(Task_Number);
                        taskWaitingQueue.insert(info);
                        String newjobinfo = String.valueOf(Task_Number)+"-"+String.valueOf(i+1);
                        zkc.getZooKeeper().setData(jobPath,newjobinfo.getBytes(), -1);
                    }
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void workerGroupWatchHandle(WatchedEvent event) {
        try {
            System.out.println("Reset watch");
            zkc.getZooKeeper().getChildren("/workersGroup", workerGroupWatch);
            List<String> allworkers = zkc.getZooKeeper().getChildren("/workersGroup", null);
            for (String eachworkers: allworkers){
                //reset watchers
                String workerpath = "/workersGroup/"+ eachworkers;
                zkc.getZooKeeper().exists(workerpath,workerwatcher);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void workerWatcherHandler(WatchedEvent event) {
        // When woker fail
        System.out.println("Recieving a failing worker failing");
        //reset watcher
        try {
            System.out.println("reset worker watcher");
            String failpath = event.getPath();
            System.out.println("event path " + failpath);
            String failname = failpath ;
            System.out.println("Fail Name:"+failname);

            Watcher.Event.EventType type = event.getType();
            System.out.println("The type of message is "+type.toString());
            if (type == Watcher.Event.EventType.NodeDeleted) {
                //One worker fail
                System.out.println("Confirming Recieving a failing worker failing");
                List<String> allProcessingTasks = zkc.getZooKeeper().getChildren("/taskProcessQueue", null);
                for (String eachTask : allProcessingTasks) {
                    String taskdata = new String (zkc.getZooKeeper().getData("/taskProcessQueue/"+eachTask,null,null));
                    String[] tasktd = taskdata.split("=");
                    String eachworker = tasktd[0];
                    if (failname.equals(eachworker)) {
                        System.out.println("Worker "+failname +"Fail");
                        String acpath = "/taskProcessQueue/" +eachTask;
                       // String failpathdata = new String(zkc.getZooKeeper().getData(acpath, null, null));
                        //String[] failpathd = failpathdata.split("=");
                        String taskinfo = tasktd[1];
                        System.out.println("The job info is "+taskinfo);
                        taskProcessingQueue.deletePath(acpath);
                        taskWaitingQueue.insert(taskinfo);
                    }

                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }





    }

    private void checkTaskProcessingQueue() {
        //we only look for the one
        try {
            List<String> allProcessingTasksworker = zkc.getZooKeeper().getChildren("/taskProcessQueue", null);
            List<String> allworkers = zkc.getZooKeeper().getChildren("/workersGroup", null);
            for (String Tasksworkername : allProcessingTasksworker) {
                    if (allworkers.contains(Tasksworkername)){


                    }else{
                        //that means the processing Tasks is not running anymore and needed to be removed and put back to taskwaitinqueue
                        //The path for Noting the task of a failed worker
                        String failpath = "/taskProcessQueue/" + Tasksworkername;
                        String failpathdata = new String(zkc.getZooKeeper().getData(failpath, null, null));
                        String[] failpathd = failpathdata.split("=");
                        String taskinfo = failpathd[1];
                        taskProcessingQueue.deletePath(failpath);
                        taskWaitingQueue.insert(taskinfo);
                    }



            }


        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    private void checkpath() {


        //this one will only be created once
        Stat jobrootsta = zkc.exists(jobsRoot, watcher);
        if (jobrootsta == null) {
            System.out.println("Creating " + jobsRoot);
            KeeperException.Code ret = zkc.create(
                    jobsRoot,         // Path of znode
                    null,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("successfully create the root for Jobs!");
        }

        //the worker path will only be created once
        Stat workeroot = zkc.exists("/workersGroup", workerwatcher);
        if (workeroot == null) {
            System.out.println("Creating " + "/workersGroup");
            KeeperException.Code ret = zkc.create(
                    "/workersGroup",         // Path of znode
                    null,           // Data not needed.
                    CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("successfully create the root for Workers!");
        }




        Stat stat = zkc.exists(root, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + root);
            KeeperException.Code ret = zkc.create(
                    root,         // Path of znode
                    selfAddress + ":" + selfport,           // Data not needed.
                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("the boss!");
            checkTaskProcessingQueue();
            checkUnfinishedSplitJobs();

            // after becoming the boss,then, we could let it perform the jobRequest function
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            new jobRequestHandlingThread(serverSocket.accept(), zkc, taskWaitingQueue, taskProcessingQueue).start();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();



            workerGroupWatch = new Watcher() { // Anonymous Watcher for the JobTracker Primary Purpose
                @Override
                public void process(WatchedEvent event) {
                    workerGroupWatchHandle(event);
                }
            };

            workerwatcher = new Watcher() { // Anonymous Watcher for the JobTracker Primary Purpose
                @Override
                public void process(WatchedEvent event) {
                    workerWatcherHandler(event);
                }
            };


            try {
                List<String> allworkers = zkc.getZooKeeper().getChildren("/workersGroup", workerGroupWatch);
                for (String eachworkers: allworkers){
                    //set all the individual watcher
                    String workerpath = "/workersGroup/"+ eachworkers;
                    zkc.getZooKeeper().exists(workerpath,workerwatcher);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }


    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        Watcher.Event.EventType type = event.getType();
        if (path.equalsIgnoreCase(root)) {
            if (type == Watcher.Event.EventType.NodeDeleted) {
                System.out.println(root + " deleted! Let's go!");
                checkpath(); // try to become the boss
            }
            if (type == Watcher.Event.EventType.NodeCreated) {
                System.out.println(root + " created!");
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                }
                checkpath(); // re-enable the watch
            }
        }
    }
}

class jobRequestHandlingThread extends Thread {
    Socket requestSocket;
    ZooKeeperConnector zkc;
    Watcher successwatcher;
    Watcher failwatcher;
    Watcher workerwatcher;
    DataOutputStream objectOutputStream;

    ZookeeperQueue taskWaitingQueue;
    ZookeeperQueue taskProcessingQueue;

    public jobRequestHandlingThread(Socket requestSocket, ZooKeeperConnector zkc, ZookeeperQueue taskWaitingQueue, ZookeeperQueue taskProcessingQueue) {
        this.requestSocket = requestSocket;
        this.zkc = zkc;
        this.taskProcessingQueue = taskProcessingQueue;
        this.taskWaitingQueue = taskWaitingQueue;

        System.out.println("Construct a jobRequestHandlingThread");
        /*
        successwatcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                successTaskHandleEvent(event);
            }
        };

        failwatcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                failTasksHandleEvent(event);
            }
        };*/


        try {
            zkc.getZooKeeper().getChildren("/workersGroup", workerwatcher);
        } catch (Exception e) {
            System.out.println("Cannot set watch on the worker thread");
        }


    }

    public void deleteJob(String jobName) {
        String path = "/jobs/" + jobName;
        String successpath = "/jobs/" + jobName + "/success";
        String failpath = "/jobs/" + jobName + "/notFound";
        try {
            List<String> children = zkc.getZooKeeper().getChildren(successpath, false);
            for (String child : children) {
                zkc.getZooKeeper().delete(successpath + "/" + child,-1);
            }
            zkc.getZooKeeper().delete(successpath, -1);

            List<String> failchildren = zkc.getZooKeeper().getChildren(failpath, false);
            for (String child : failchildren) {
                zkc.getZooKeeper().delete(failpath + "/" + child,-1);
            }
            zkc.getZooKeeper().delete(failpath, -1);

            zkc.getZooKeeper().delete(path,-1);

        }
        catch (KeeperException.NoNodeException e) {
            //System.out.printf("Group %s does not exist\n", groupName);
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    private void checkJobState(String requestword) {
        //we need to check whether some of the jobs have been finished during the time when it is crashed and restablish a new primary job Tracker
        try {
            List<String> alljobs = zkc.getZooKeeper().getChildren("/jobs", null);
            for (String jobname : alljobs) {

                if (jobname.equals(requestword)) {
                    String jobpath = "/jobs/" + jobname;
                    String jobinfo = new String(zkc.getZooKeeper().getData(jobpath, null, null));
                    String [] jobi = jobinfo.split("-");
                    int Task_Number = Integer.parseInt(jobi[0]);
                    int notFoundNumber = zkc.getZooKeeper().getChildren(jobpath + "/notFound", null).size();
                    List <String> successelements = zkc.getZooKeeper().getChildren(jobpath + "/success", null);
                    int successNumber = successelements.size();

                    if (Task_Number == notFoundNumber + successNumber) {
                        //Now we can respond to the right client
                        String message = "";
                        if (successNumber == 1) {
                            Stat s = null;
                            byte[] b=zkc.getZooKeeper().getData(jobpath + "/success/"+successelements.get(0), null, s);
                            String password = new String(b);
                            message = "Password found:" + password + "\r\n";
                        } else {
                            message = "Failed:Password not found\r\n";
                        }

                        objectOutputStream.writeBytes(message);
                        //we delete the job in the zookeeper
                        deleteJob(jobname);

                    }
                    else{
                        String message = "In progress\r\n";
                        objectOutputStream.writeBytes(message);
                    }

                    return;
                }

            }
            //that means the job is not found
            objectOutputStream.writeBytes("Job not found\r\n");

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }




    private void splitJobs(String word, int worker_number, String jobPath) {
        try {
            for (int i = 0; i < worker_number; i++) {
                String info = "";
                info = word + "-" + String.valueOf(i + 1) + ":" + String.valueOf(worker_number);
                taskWaitingQueue.insert(info);
                String jobinfo = String.valueOf(worker_number)+"-"+String.valueOf(i+1);
                zkc.getZooKeeper().setData(jobPath,jobinfo.getBytes(), -1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public String slice_end(String s, int endIndex) {
        if (endIndex < 0) endIndex = s.length() + endIndex;
        return s.substring(0, endIndex);
    }

/*
    private void failTasksHandleEvent(WatchedEvent event) {
        System.out.println("triggered job not found event");
        String path = event.getPath();
        System.out.println("I recieved a not found message of path:" + path);
        String pattern = "/notFound";
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(path);
        int startindex = 1;
        if (m.find()) {
            startindex = m.start();
        }
        //only obtain the path of that specific job
        String jobpath = slice_end(path, startindex);
        Watcher.Event.EventType type = event.getType();
        System.out.println("type is " + type.toString());
        if (type == Watcher.Event.EventType.NodeChildrenChanged) {
            try {
                //now we look for the the number of notFound case
                String jobdata = new String(zkc.getZooKeeper().getData(jobpath, null, null));
                String[] jdata = jobdata.split("-");
                int Task_number = Integer.parseInt(jdata[0]);
                String client = jdata[1];
                int notFoundNumber = zkc.getZooKeeper().getChildren(path, null).size();

                // now we need to look for the number of success case
                int successNumber = zkc.getZooKeeper().getChildren(jobpath + "/success", null, null).size();
                System.out.println("Notfound:" + notFoundNumber + '\n');
                System.out.println("SuccessNumber:" + successNumber + '\n');
                System.out.println("Task_Number:" + Task_number + '\n');


                if (notFoundNumber + successNumber == Task_number) {
                    //that mean, we are already collect all Tasks for this jobs and we will be able to deleteData
                    if (successNumber == 0) {
                        System.out.println("job finished but not found password");
                        objectOutputStream.writeBytes("Job finished & the password is not Found \r\n");
                    }
                    //we can deleteData the job node


                } else {
                    //reset watcher when the jobs are not finished
                    try {
                        zkc.getZooKeeper().getChildren(path, workerwatcher);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // reset watcher if something bad happens
            try {
                zkc.getZooKeeper().getChildren(path, failwatcher);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }*/

    /*
    private void successTaskHandleEvent(WatchedEvent event) {

        String path = event.getPath();
        System.out.println("I recieved a success message of path:" + path);
        String pattern = "/success";
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(path);
        int startindex = 1;
        if (m.find()) {
            startindex = m.start();
        }
        //only obtain the path of that specific job
        String jobpath = slice_end(path, startindex);
        Watcher.Event.EventType type = event.getType();
        if (type == Watcher.Event.EventType.NodeChildrenChanged) {
            try {
                String jobdata = new String(zkc.getZooKeeper().getData(jobpath, null, null));
                String[] jdata = jobdata.split("-");
                int Task_number = Integer.parseInt(jdata[0]);
                int notFoundNumber = zkc.getZooKeeper().getChildren(jobpath + "/notFound", null).size();
                if (notFoundNumber + 1 == Task_number) {
                    //that mean, we are already collect all Tasks for this jobs and we will be able to deleteData
                    //we can deleteData the job node
                    String password = new String(zkc.getZooKeeper().getData(path, null, null));
                    //aslo we can send the back the successful message to
                    objectOutputStream.writeBytes("Job finished & the password:" + password + "\r\n");
                } else {
                    //reset watchwer because the jobs are not finshed yet
                    try {
                        zkc.getZooKeeper().getChildren(path, successwatcher);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // reset watcher if something else happen
            try {
                zkc.getZooKeeper().getChildren(path, successwatcher);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // need to send back to clients for Found

    }*/





    @Override
    public void run() {
        System.out.println("Begin to Run");
        try {
            objectOutputStream = new DataOutputStream(requestSocket.getOutputStream());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(requestSocket.getInputStream()));
            String request = bufferedReader.readLine();
            System.out.println("I receiving " + request);

            String r[] = request.split("-");
            String requestword = r[1];
            String status = r[0];


            if (status.equals("status")) {
                checkJobState(requestword);
            } else {
                //first we need to check if the same word exist before
                boolean createjob = true;
                List<String> alljobs = zkc.getZooKeeper().getChildren("/jobs", null);
                for (String jobname : alljobs) {
                    if (jobname.equals(requestword)) {
                        deleteJob(requestword);
                    }
                }

                if (createjob) {

                    //Now we need to create a node for a job that request to crack the word , 'requestword'
                    String path = "/jobs/" + requestword ;
                    //Stat stat = zkc.exists(path, jobswatcher);


                    // get the number of worker
                    List workers = zkc.getZooKeeper().getChildren("/workersGroup", true);
                    int worker_number = workers.size();
                    String jobInfo = String.valueOf(worker_number) + "-0";
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
                    splitJobs(requestword, worker_number,path);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
