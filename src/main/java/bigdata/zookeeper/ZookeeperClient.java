package bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient {
    private static String connectString = "APPLE:2181,cedar:2181,roc15:2181";
    private static int sessionTimeout = 5000;
    private static ZooKeeper zk = null;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static Stat stat = new Stat();

    public static void getConn() throws IOException {
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
                    try {
                        if(event.getType() == Event.EventType.None && event.getPath() == null){
                            System.out.println("Zookeeper客户端与服务端成功建立连接！");
                            countDownLatch.countDown();
                        }else if(event.getType() == Event.EventType.NodeChildrenChanged){
                            System.out.println("通知：" + event.getPath() + "节点的子节点发生变化！");

                            ZookeeperClient.getChildrenNodes(event.getPath(),true);

                        }else if(event.getType() == Event.EventType.NodeDataChanged){
                            System.out.println("通知：" + event.getPath() + "节点数据发生变化！");
                            ZookeeperClient.getZnodeData(event.getPath(),true);//重新注册监听事件
                        }else if(event.getType() == Event.EventType.NodeCreated){
                            System.out.println("通知：" + event.getPath() + "节点被创建");
                            ZookeeperClient.existsZnode(event.getPath(),true);//重新注册监听事件
                        }else if(event.getType() == Event.EventType.NodeDeleted){
                            System.out.println("通知：" + event.getPath() + "节点被删除");
                            ZookeeperClient.existsZnode(event.getPath(),true);//重新注册监听事件
                        }

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        });

    }

    //创建节点
    public static void createZnode(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
        if(zk != null){
            //String path：znode路径
            //byte[] data：znode值
            //List<ACL> acl：znode访问权限
            //CreateMode createMode：znode类型
            zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,createMode);
        }
    }

    //获取节点下所有子节点
    public static void getChildrenNodes(String path,boolean watch) throws KeeperException, InterruptedException {
        if(zk != null){
            List<String> children = zk.getChildren(path,watch);
            System.out.println(path + "节点的所有子节点:" + children);
        }
    }

    //获取节点数据
    public static void getZnodeData(String path,boolean watch) throws KeeperException, InterruptedException {
        if(zk != null){
            //String path：节点路径
            //boolean watch：是否注册监听事件
            //Stat stat：节点状态信息对象
            byte[] data = zk.getData(path,watch,stat);
            System.out.println("获取到的" + path + "节点数据：" + new String(data));
            System.out.println("czxid:" + stat.getCzxid() + ",mzxid:" + stat.getMzxid());
        }
    }

    //更新节点数据
    public static Stat setZnodeData(String path,byte[] data,int version) throws KeeperException, InterruptedException {
        Stat stat1 = null;
        if(zk != null){
            //String path：节点路径
            //byte[] data：待更新数据
            //int version：更新的数据版本号（dataVersion），version=-1表示基于znode最新的版本号进行更新
            //             version从0开始，更新操作传入的版本号要与节点最新的数据版本号一致，否则更新失败

            stat1 = zk.setData(path,data,version);
            System.out.println(path + "节点信息：czxid=" + stat1.getCzxid()
                    + ",mzxid=" + stat1.getMzxid()
                    + ",dataVersion=" + stat1.getVersion());
        }
        return stat1;
    }

    //删除节点
    public static void deleteZnode(String path,int version) throws KeeperException, InterruptedException {
        if(zk != null){
            //String path：删除的节点路径
            //int version：要删除的节点的版本号
            //只允许删除叶子节点，不能删除带有子节点的嵌套节点

            zk.delete(path,version); //delete命令
            System.out.println("已删除" + path + "节点！");
        }
    }

    //检查节点是否存在
    public static void existsZnode(String path,boolean watch) throws KeeperException, InterruptedException {
        if(zk != null){
            //String path：待检查节点路径
            //boolean watch：是否注册监听事件
            Stat statrs = zk.exists(path,watch);
            if(statrs == null){
                System.out.println(path + "节点不存在!");
            }
        }
    }

    public static void main(String[] args){
        try {
            ZookeeperClient.getConn();
            System.out.println("当前连接状态：" + zk.getState());

            countDownLatch.await(); //等待服务端向客户端发送成功建立连接的通知
            //1）创建znode节点
            //注意：创建同名节点会抛出异常NodeExistsException: KeeperErrorCode = NodeExists for /zk-test1
            String path = "/zk-test1";
            ZookeeperClient.createZnode(path,"hello zktest1".getBytes(),CreateMode.PERSISTENT);
            //注意：不能够创建包含子节点的嵌套节点NoNodeException: KeeperErrorCode = NoNode for /zk-test2/temp1
//            String path = "/zk-test2/temp1";
//            ZookeeperClient.createZnode(path,"000".getBytes(),CreateMode.PERSISTENT);

            //2）获取节点下的所有子节点
            ZookeeperClient.getChildrenNodes("/zk-test1",true);
            ZookeeperClient.createZnode("/zk-test1/tmp1","hello tmp1".getBytes(),CreateMode.PERSISTENT);

            //3）获取节点数据
            ZookeeperClient.getZnodeData("/zk-test1",true);

            //4)跟新节点数据
            //Stat stat1 = ZookeeperClient.setZnodeData("/zk-test1","111".getBytes(),-1);
            //Stat stat2 = ZookeeperClient.setZnodeData("/zk-test1","222".getBytes(),stat1.getVersion());
            //注意：更新操作使用的版本号与节点最新版本号不一致，抛出异常BadVersionException: KeeperErrorCode = BadVersion for /zk-test1
            //Stat stat3 = ZookeeperClient.setZnodeData("/zk-test1","333".getBytes(),stat1.getVersion());

            //5)删除节点
            //zk-test1下有子节点，删除失败，抛出异常NotEmptyException: KeeperErrorCode = Directory not empty for /zk-test1
            //ZookeeperClient.deleteZnode("/zk-test1",4);
            //传入删除的版本号与实际版本号不一致，删除失败，抛出异常BadVersionException: KeeperErrorCode = BadVersion for /zk-test1/tmp1
            //ZookeeperClient.deleteZnode("/zk-test1/tmp1",1);
//            ZookeeperClient.deleteZnode("/zk-test1/tmp1",0);
            //ZookeeperClient.deleteZnode("/zk-test1",4);

            //6)检查节点是否存在
            ZookeeperClient.existsZnode("/zk-test1",true);

            Thread.sleep(Integer.MAX_VALUE);
            System.out.println("客户端运行结束！");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
