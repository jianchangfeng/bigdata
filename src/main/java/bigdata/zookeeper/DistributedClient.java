package bigdata.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class DistributedClient {
    private static final String connectString = "node01:2181,node02:2181,node03:2181";
    private static final int sessionTimeout = 5000;
    private ZooKeeper zk = null;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static final String rootNode = "/servers";


    private static List<String> runningServerList = null; //正在运行的服务器列表


    private static String connectedServer;//客户端已经连接的服务器地址

    private static int clientNumber;//客户端编号


    public void getConn() throws IOException {
        //String connectString, int sessionTimeout, Watcher watcher
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //作用：客户端接收服务端发送的监听通知事件
                System.out.println("收到监听通知！");
                System.out.println("type:" + event.getType() + ",path:" + event.getPath());

                if(event.getState() == Event.KeeperState.SyncConnected){
                    //注意：客户端向服务端注册的Watcher是一次性的，一旦服务端向客户端发送一次通知后，这个Watcher失效，
                    //     客户端需要反复注册Watcher
                    //服务端向客户端发送成功建立连接的通知

                    if(event.getType() == Event.EventType.None && event.getPath() == null){
                        System.out.println("Zookeeper客户端与服务端成功建立连接！");
                        countDownLatch.countDown();
                    }else if(event.getType() == Event.EventType.NodeChildrenChanged){
                        System.out.println("重新获取服务器列表，并注册监听事件！");
                        try {
                            getRunningServers(event.getPath());//重新获取服务器列表，并注册监听事件
                        } catch (KeeperException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
        });

    }

    /**
     * 获取正在运行的服务器列表
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void getRunningServers(String path) throws KeeperException, InterruptedException {
        runningServerList = zk.getChildren(path,true);
        System.out.println("Running servers:" + runningServerList);
    }


    /**
     * 获取客户端要连接的服务器地址
     * @param clientNumber
     * @return
     */
    public String getTargetServerAddress(int clientNumber){
        if(runningServerList != null && runningServerList.size() > 0){
            System.out.println("clientNumber:" + clientNumber);
            System.out.println("running servers:" + runningServerList.size());
            //算法：targetServer = clientNumber % runningServerlist.size
            int serverIndex = clientNumber % runningServerList.size(); //目标服务器在正在运行的服务器列表中的位置
            return runningServerList.get(serverIndex);//获取目标服务器的访问地址
        }else {
            return null;
        }
    }

    /**
     * 获取客户端编号
     * @return
     */
    public int getClientNumber(){
        Random random = new Random();
        return random.nextInt(1000);
    }

    /**
     * 检测客户端已经连接的服务器地址是否有效
     * @return
     */
    public boolean ifConnected(){
        if(runningServerList != null && runningServerList.size() > 0 && connectedServer != null){
            for(String server : runningServerList){
                if(connectedServer.equals(server)) return true;
            }
        }
        return false;
    }


    /**
     * 执行具体的业务功能，每隔2秒钟监测已连接的服务器地址是否有效，无效则重新获取服务器地址
     * @param serverUrl
     * @throws InterruptedException
     */
    public void work(String serverUrl) throws InterruptedException {
        System.out.println("serverUrl:" + serverUrl);
        System.out.println("client working ...");
        connectedServer = serverUrl;
        while (true){
            System.out.println("connectedServer：" + connectedServer);
            Thread.sleep(2000);
            if(!ifConnected()){
                System.out.println(connectedServer + "连接失效，重新获取服务器地址！");
                connectedServer = getTargetServerAddress(clientNumber);
                System.out.println("重新连接：" + connectedServer + "成功！");
            }
        }
    }

    public static void main(String[] args){
        //获取zk连接
        DistributedClient client = new DistributedClient();
        try {
            client.getConn();
            countDownLatch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //从zk获取正在运行的服务器列表
        try {
            client.getRunningServers(rootNode);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //获取客户端要访问的服务器地址
        clientNumber = client.getClientNumber();
        String targetUrl = client.getTargetServerAddress(clientNumber);

        //执行具体的业务功能
        try {
            client.work(targetUrl);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
