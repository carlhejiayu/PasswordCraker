import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

/**
 * Created by herbert on 2016-03-26.
 */
public class ZookeeperQueue {
    static CountDownLatch countDownLatch = new CountDownLatch(1);
    ZooKeeperConnector zooKeeperConnector;
    String queueName;

    public ZookeeperQueue(String queueName, ZooKeeperConnector zooKeeperConnector) {
        zooKeeperConnector = new ZooKeeperConnector();
        this.zooKeeperConnector = zooKeeperConnector;
        this.queueName = "/" + queueName;

    }

    public void tryCreate(){
        // Create ZK node name
        if (zooKeeperConnector != null) {
            Stat s = zooKeeperConnector.exists(queueName, null);
            if (s == null) {
                zooKeeperConnector.create(queueName, null, CreateMode.PERSISTENT);
            }
        }
    }


    public boolean insert(String data) throws KeeperException, InterruptedException {

        zooKeeperConnector.create(queueName + "/element", data, CreateMode.PERSISTENT_SEQUENTIAL);
        return true;
    }

    public void delete(String data){
        Stat stat = null;

        // Get the first element available
        while (true) {

            ArrayList<String> list = null;
            try {
                list = (ArrayList<String>)
                        zooKeeperConnector.getZooKeeper().getChildren(queueName, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                countDownLatch.countDown();
                            }
                        });
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (list.isEmpty()) {
                System.out.println("Nothing to delete");
                return;
            } else {
                Integer todelete = new Integer(-1);

                for (String s : list) {
                    Integer tempValue = new Integer(s.substring(7));
                    String d = null;
                    try {
                        byte[] b = zooKeeperConnector.getZooKeeper().getData(queueName + "/element" + tempValue, false, stat);
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append(b);
                        d = stringBuilder.toString();

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(d.equals(data)) {
                        todelete = tempValue;
                        break;
                    }

                }


                try {
                    zooKeeperConnector.getZooKeeper().delete(queueName + "/element" + todelete, 0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    //cannot delete because other has delete it
                    //has to retry !!!!!!!!!!
                    System.out.println("already delete");
                    //e.printStackTrace();
                }
                return;
            }

        }
    }

    public void deletePath(String path){
        try {
            zooKeeperConnector.getZooKeeper().delete(path, 0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    public String pop() {
        String retvalue = null;
        Stat stat = null;

        // Get the first element available
        while (true) {

            ArrayList<String> list = null;
            try {
                list = (ArrayList<String>)
                        zooKeeperConnector.getZooKeeper().getChildren(queueName, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                countDownLatch.countDown();
                            }
                        });
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (list.isEmpty()) {
                System.out.println("Going to wait");
                countDownLatch = new CountDownLatch(1);
                try {
                    countDownLatch.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                Integer min = new Integer(list.get(0).substring(7));
                for (String s : list) {
                    Integer tempValue = new Integer(s.substring(7));
                    if (tempValue < min) min = tempValue;
                }
                System.out.println("Temporary value: " + queueName + "/element" + min);
                byte[] b = new byte[0];
                try {
                    b = zooKeeperConnector.getZooKeeper().getData(queueName + "/element" + min, false, stat);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    zooKeeperConnector.getZooKeeper().delete(queueName + "/element" + min, 0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    //cannot delete because other has delete it
                    //has to retry !!!!!!!!!!
                    System.out.println("delete fail and try to pop again");
                    return pop();
                    //e.printStackTrace();
                }
                StringBuilder buffer = new StringBuilder();
                buffer.append(b);
                retvalue = buffer.toString();

                return retvalue;
            }

        }
    }


}
