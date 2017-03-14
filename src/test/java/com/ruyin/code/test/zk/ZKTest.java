package com.ruyin.code.test.zk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by ruyin on 2017/3/13.
 *
 * ZK的java api操作
 */
public class ZKTest {

    private static final int SESSION_TIMEOUR = 3000;

    private static Logger logger = Logger.getLogger(ZKTest.class);

    private ZooKeeper zooKeeper;

    private Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            logger.info("Process:" + event.getType());
        }
    };

    @Before
    public void init() throws IOException {
        //单机版的Zookeeper,connectString支持以","结束的主机列表,适用于集群
        zooKeeper = new ZooKeeper("192.168.2.130:2181",SESSION_TIMEOUR,watcher);
    }

    @After
    public void destroy() throws InterruptedException {
        zooKeeper.close();
    }

    //创建一个Znode
    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        String result = zooKeeper.create("/zk","znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        logger.info("Create result:"+result);
        System.out.println(result);
    }


    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        //version为-1表示匹配任意的version
        zooKeeper.delete("/zk",-1);
    }

    @Test
    public void testGetChildren() throws KeeperException, InterruptedException {

        zooKeeper.create("/pig",null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        zooKeeper.create("/pig/gg1","gg1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        zooKeeper.create("/pig/gg2","gg2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

        List<String> list = zooKeeper.getChildren("/pig",true);
        logger.info("Get children:" + list);
        list.stream().forEach(node -> System.out.println(node));
    }

    @Test
    public void testGetNode() throws KeeperException, InterruptedException {
        /*zooKeeper.getData("/zk", true, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

            }
        },null);*/

        byte[] bytes = zooKeeper.getData("/zk",null,null);
        String result = new String(bytes);
        logger.info("Get Node:" + result);
        System.out.println(result);
    }

    @Test
    public void testNodeExist() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists("/zk",false);
        System.out.println(stat);
    }


}
