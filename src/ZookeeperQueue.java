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
        this.zooKeeperConnector = zooKeeperConnector;
        this.queueName = "/" + queueName;

    }

    public void tryCreate() {
        // Create ZK node name
        if (zooKeeperConnector != null) {
            Stat s = zooKeeperConnector.exists(queueName, null);
            if (s == null) {
                zooKeeperConnector.create(queueName, null, CreateMode.PERSISTENT);
            }
        }
    }


    public String insert(String data) throws KeeperException, InterruptedException {

        String path = zooKeeperConnector.createReturnPath(queueName + "/element", data, CreateMode.PERSISTENT_SEQUENTIAL);
        return path;
    }

    public int insertAndGetSequence(){
        String path = zooKeeperConnector.createReturnPath(queueName + "/element", "sequence", CreateMode.PERSISTENT_SEQUENTIAL);
        return Integer.parseInt(path.substring(7));
    }


    public void deleteData(String data) {
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
                System.out.println("Nothing to deleteData");
                return;
            } else {
                String todelete = null;

                for (String s : list) {
                    String tempString = s;
                    String d = null;
                    try {
                        byte[] b = zooKeeperConnector.getZooKeeper().getData(queueName + "/" + tempString, false, stat);

                        d = new String(b);

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (d.equals(data)) {
                        todelete = tempString;
                        break;
                    }

                }


                try {
                    zooKeeperConnector.getZooKeeper().delete(queueName + "/" + todelete, 0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    //cannot deleteData because other has deleteData it
                    //has to retry !!!!!!!!!!
                    System.out.println("already deleteData");
                    //e.printStackTrace();
                }
                return;
            }

        }
    }

    public void deletePath(String path) {
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


        ArrayList<String> list = null;
        while (list == null || list.isEmpty()) {
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
                System.out.println(queueName + " queue is empty. Going to wait");
                countDownLatch = new CountDownLatch(1);
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //Integer min = new Integer(list.get(0).substring(7));
        /*for (String s : list) {
            Integer tempValue = new Integer(s.substring(7));
            if (tempValue < min) min = tempValue;
        }*/
        //System.out.println("Temporary value: " + queueName + "/element" + min);

        String firstChild = list.get(0);
        System.out.println("firstChild is " + firstChild);
        byte[] b = new byte[0];
        try {
            b = zooKeeperConnector.getZooKeeper().getData(queueName + "/"+firstChild, null, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            zooKeeperConnector.getZooKeeper().delete(queueName + "/"+firstChild, 0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            //cannot deleteData because other has deleteData it
            //has to retry !!!!!!!!!!
            e.printStackTrace();
            System.out.println("deleteData fail and try to pop again");
            return pop();

        }

        retvalue = new String(b);

        return retvalue;
    }
}
