import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by herbert on 2016-03-26.
 */
public class Worker {
    static CountDownLatch nodeCreateSignal = new CountDownLatch(1);
    ZooKeeperConnector zooKeeperConnector;
    String workerGroupPath = "/workersGroup";
    String myPath = "/worker";

    public void createSelfNode(){
        KeeperException.Code ret = zooKeeperConnector.create(
                workerGroupPath + myPath,         // Path of znode
                null,           // Data not needed.
                CreateMode.EPHEMERAL_SEQUENTIAL   // Znode type, set to EPHEMERAL SEQUENTIAL.
        );
        if (ret == KeeperException.Code.OK) System.out.println("create the worker!");
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
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(b);
                String address = stringBuilder.toString();
                System.out.println("address is: " + address);
                IpAddress ipAddress = IpAddress.parseAddressString(address);


            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
