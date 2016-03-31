import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by herbert on 2016-03-24.
 */
public class FileServer {
    static CountDownLatch nodeCreateSignal = new CountDownLatch(1);
    static String myPath = "/fileServer";
    ZooKeeperConnector zooKeeperConnector;
    Watcher watcher;
    ArrayList dictionary;
    int dictionarySize;
    ServerSocket serverSocket;
    int selfPort;
    String selfAddress;
    public static void main(String[] args){
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Test zkServer:clientPort filename");
            return;
        }

        FileServer fileServer = new FileServer(args[0], 8000, args[1]);


        fileServer.checkpath();

        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }

    private void checkpath() {
        Stat stat = zooKeeperConnector.exists(myPath, watcher);

        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating fileserver at: " + myPath);
            KeeperException.Code ret = zooKeeperConnector.create(
                    myPath,         // Path of znode
                    selfAddress+":"+ selfPort,           // Data not needed.
                    CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
            );
            if (ret == KeeperException.Code.OK) System.out.println("create the fileserver!");
        }
    }

    public FileServer(String hosts, int selfPort, String filename) {
        zooKeeperConnector = new ZooKeeperConnector();
        try {
            zooKeeperConnector.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }

        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);

            } };

        //get self address
        this.selfPort = selfPort;
        try {
            this.selfAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        //read dictionary from file
        // FileReader reads text files in the default encoding.
        FileReader fileReader =
                null;
        try {
            fileReader = new FileReader(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // Always wrap FileReader in BufferedReader.
        BufferedReader bufferedReader =
                new BufferedReader(fileReader);

        String line;
        List list = new LinkedList<String>();
        System.out.println("loading file, please wait...");
        try {
            while((line = bufferedReader.readLine()) != null) {
                list.add(line);
                //System.out.println(line);
            }
            dictionary = new ArrayList<>(list);
            dictionarySize = dictionary.size();
            serverSocket = new ServerSocket(selfPort);


        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("finish loading file");
        final FileServer fileServer = this;

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    try {
                        new requestHandlingThread(serverSocket.accept(), fileServer).start();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

    }
    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        Watcher.Event.EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == Watcher.Event.EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! backup fileserver go!");
                checkpath(); // try to become the boss
            }
            if (type == Watcher.Event.EventType.NodeCreated) {
                System.out.println(myPath + " created! fileserver");
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }


}

class requestHandlingThread extends Thread{
    Socket requestSocket;
    FileServer fileServer;

    public requestHandlingThread(Socket requestSocket, FileServer fileServer) {
        this.requestSocket = requestSocket;
        this.fileServer = fileServer;
    }

    @Override
    public void run() {
        try {
            BufferedReader bufferedReader= new BufferedReader(new InputStreamReader(requestSocket.getInputStream()));
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(requestSocket.getOutputStream());
            String requestfile = bufferedReader.readLine();
            //parse the string

            int numerator = 0;
            int denominator = 0;
            String[] s = requestfile.split(":");
            numerator = Integer.parseInt(s[0]);
            denominator = Integer.parseInt(s[1]);

            int partitionSize = Math.round((float)fileServer.dictionarySize / denominator);
            List partition = new ArrayList<>();
            if(numerator != denominator){
                 partition = fileServer.dictionary.subList((numerator-1)*partitionSize, numerator*partitionSize);
            }
            else if(numerator == denominator){
                //the last one take all from the start of index to last
                partition = fileServer.dictionary.subList((numerator-1)*partitionSize, fileServer.dictionarySize);
            }
            partition = new ArrayList<>(partition);
            System.out.println("size of partition sent: " + partition.size());
            objectOutputStream.writeObject(partition);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
