package bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class CheckConfig_Client {
    private static String connectString = "cedar:2181,APPLE:2181,roc15:2181";
    private static int sessionTimeout = 5000;
    private static ZooKeeper zk = null;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static String serverNodeName = "/jdbc";
    private static Stat stat = new Stat();

    public static void getConn() throws IOException {
        //String connectString, int sessionTimeout, Watcher watcher
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //作用：客户端接收服务端发送的监听通知事件
                System.out.println("收到监听通知！");
                System.out.println("type:" + event.getType() + ",path:" + event.getPath());
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    //注意：客户端向服务端注册的Watcher是一次性的，一旦服务端向客户端发送一次通知后，这个Watcher失效，
                    //     客户端需要反复注册Watcher
                    //服务端向客户端发送成功建立连接的通知
                    if (event.getType() == Event.EventType.None && event.getPath() == null) {
                        System.out.println("Zookeeper2客户端与服务端成功建立连接！");
                        countDownLatch.countDown();
                    }
                }
            }
        });
    }

    //获取节点数据并转换为String类型
    public static String getZnodeData(String path, boolean watch) throws KeeperException, InterruptedException {
        String conent = null;
        if (zk != null) {
            //String path：节点路径
            //boolean watch：是否注册监听事件
            //Stat stat：节点状态信息对象
            byte[] data = zk.getData(path, watch, stat);
            conent = new String(data);
            System.out.println("获取到的" + path + "jdbc节点数据：" + conent);
            System.out.println("czxid:" + stat.getCzxid() + ",mzxid:" + stat.getMzxid());
        }
        return conent;
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        //获取zk连接
        CheckConfig_Client CCclient = new CheckConfig_Client();
        try {
            CCclient.getConn();
            System.out.println("当前连接状态：" + zk.getState());
            countDownLatch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //3）将获取的节数据，write到配置文件
        while(true){
            Thread.sleep(5000);
            String file_conent = CCclient.getZnodeData(serverNodeName,true);
            FileOutputStream stream  = new FileOutputStream(new File("jdbc.txt"));
            stream.write(file_conent.getBytes("utf-8"));
            stream.close();
        }
    }

}


