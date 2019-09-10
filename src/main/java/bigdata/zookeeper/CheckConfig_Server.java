package bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class CheckConfig_Server {
    private static final String connectString = "cedar:2181,APPLE:2181,roc15:2181";
    private static final int sessionTimeout = 5000;
    private static ZooKeeper zk = null;
    private static Stat stat = new Stat();
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final String rootNode = "/";
    private static String serverNodeName = "/jdbc";
    public  void getConn() throws IOException {
        //String connectString, int sessionTimeout, Watcher watcher
        zk = new ZooKeeper(connectString, sessionTimeout, event -> {
            //作用：客户端接收服务端发送的监听通知事件
            System.out.println("收到监听通知！");
            System.out.println("type:" + event.getType() + ",path:" + event.getPath());
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                //注意：客户端向服务端注册的Watcher是一次性的，一旦服务端向客户端发送一次通知后，这个Watcher失效，
                //     客户端需要反复注册Watcher
                //服务端向客户端发送成功建立连接的通知

                if (event.getType() == Watcher.Event.EventType.None && event.getPath() == null) {
                    System.out.println("Zookeeper客户端与服务端成功建立连接！");
                    countDownLatch.countDown();
                }
            }
        });
    }

    /**
     * 服务端程序向zk注册永久节点
     */
    public void RegisterPermanentServerNode(String serverNodeName) throws KeeperException, InterruptedException {
        String serverNodePath = rootNode +  serverNodeName;
        String rs = zk.create(serverNodePath,serverNodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        if(rs != null){
            System.out.println(serverNodeName + "永久节点创建成功！");
        }
    }

    /**
    *检查本地配置文件jdbc.txt的数据
     */
    public byte[] Checking_Config(){
        System.out.println("Checking jdbc.txt on jdbc node ...");
        File file = new File("jdbc.txt");
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return filecontent;
    }

    /**
     * 将服务端本地的jdbc.txt文件的数据写入到/jdbc节点
     */
    public static Stat Write_Node(int version) {
        System.out.println("Writing jdbc.txt into jdbc node ...");
        Stat stat1 = null;
        File file = new File("jdbc.txt");
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
            if(zk != null) {
                //String serverNodeName：节点路径
                //filecontent：读取的内容
                //int version：更新的数据版本号（dataVersion），version=-1表示基于znode最新的版本号进行更新
                //             version从0开始，更新操作传入的版本号要与节点最新的数据版本号一致，否则更新失败
                stat1 = zk.setData(serverNodeName, filecontent, version);
                System.out.println(serverNodeName + "节点信息：czxid=" + stat1.getCzxid()
                        + ",mzxid=" + stat1.getMzxid()
                        + ",dataVersion=" + stat1.getVersion());
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return stat1;
    }
    /**
     * Update jdbc.txt文件的数据到zookeeper集群
     */
    public static Stat Update_Node(byte[] filecontent, int version) throws KeeperException, InterruptedException {
        Stat stat2 = null;
        if(zk != null){
            //String path：节点路径
            //byte[] data：待更新数据
            //int version：更新的数据版本号（dataVersion），version=-1表示基于znode最新的版本号进行更新
            //             version从0开始，更新操作传入的版本号要与节点最新的数据版本号一致，否则更新失败

            stat2 = zk.setData(serverNodeName,filecontent,version);
        }
        return  stat2;
    }
    public static void main(String[] args) throws Exception {
        //获取Zookeeper连接
        CheckConfig_Server CCserver = new CheckConfig_Server();
        CCserver.getConn();
        countDownLatch.await();

        String hostname = args.length == 0 ? "jdbc" : args[0];
        //向zk注册永久节点
        CCserver.RegisterPermanentServerNode(hostname);

        //将服务端本地的jdbc.txt文件的数据写入到/jdbc节点
        Stat stat1 = CCserver.Write_Node(-1);
        /**
         *检查本地配置文件jdbc.txt的数据，发现变化，会自动同步到zookeeper集群
         */
        byte[] filecontent1;
        byte[] filecontent2;
        int stat_version = stat1.getVersion();
        while(true){
            System.out.println("Checking node config");
            filecontent1 = CCserver.Checking_Config();
            Thread.sleep(5000);
            filecontent2 = CCserver.Checking_Config();
            if ( ! Arrays.equals(filecontent1,filecontent2) ){
                Stat stat2 = CCserver.Update_Node(filecontent2,stat_version);
                System.out.println(serverNodeName + "节点信息：czxid=" + stat2.getCzxid()
                        + ",mzxid=" + stat2.getMzxid()
                        + ",dataVersion=" + stat2.getVersion());
                stat_version = stat2.getVersion();
            }
        }
    }
}
