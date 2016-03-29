import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by herbert on 2016-03-24.
 */
public class ZooKeeperConnector implements Watcher {
    ZooKeeper zooKeeper;

    // To block any operation until ZooKeeper is connected. It's initialized
    // with count 1, that is, ZooKeeper connect state.
    CountDownLatch connectedSignal = new CountDownLatch(1);

    // Access Control List, set to Completely Open
    protected static final List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    /**
     * Connects to ZooKeeper servers specified by hosts.
     */
    public void connect(String hosts) throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(
                hosts, // ZooKeeper service hosts
                5000,  // Session timeout in milliseconds
                this); // watcher - see process method for callbacks
        connectedSignal.await();
    }

    /**
     * Closes connection with ZooKeeper
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * @return the zooKeeper
     */
    public ZooKeeper getZooKeeper() {
        // Verify ZooKeeper's validity
        if (null == zooKeeper || !zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
            throw new IllegalStateException ("ZooKeeper is not connected.");
        }
        return zooKeeper;
    }

    protected Stat exists(String path, Watcher watch) {

        Stat stat =null;
        try {
            stat = zooKeeper.exists(path, watch);
        } catch(Exception e) {
        }

        return stat;
    }

    protected KeeperException.Code create(String path, String data, CreateMode mode) {

        try {
            byte[] byteData = null;
            if(data != null) {
                byteData = data.getBytes();
            }
            zooKeeper.create(path, byteData, acl, mode);

        } catch(KeeperException e) {
            return e.code();
        } catch(Exception e) {
            return KeeperException.Code.SYSTEMERROR;
        }

        return KeeperException.Code.OK;
    }

    protected String createReturnPath(String path, String data, CreateMode mode) {

        try {
            byte[] byteData = null;
            if(data != null) {
                byteData = data.getBytes();
            }
            String actualpath = zooKeeper.create(path, byteData, acl, mode);
            return actualpath;

        } catch(KeeperException e) {
            return null;
        } catch(Exception e) {
            return null;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // release lock if ZooKeeper is connected.
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }
}
