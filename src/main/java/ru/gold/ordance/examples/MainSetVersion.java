package ru.gold.ordance.examples;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ru.gold.ordance.CuratorFrameworks.newAutostartClient;

public class MainSetVersion {

    private static CuratorFramework CLIENT = newAutostartClient();

    public static void main(String[] args) throws Exception {
        var basePath = "/base";

//        client.create()
//                .orSetData()
//                .creatingParentsIfNeeded()
//                .storingStatIn(stat)
//                .withVersion(stat.getVersion())
//                .forPath(basePath, "Hello World".getBytes());


        Executor executor = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 120; i++) {
            executor.execute(() -> {
                try {
                    writeValue(basePath, new Value("v", 11L));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

//        executor.execute(() -> {
//            try {
//                writeValue(basePath, new Value("v", 7L));
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        });
//
//        executor.execute(() -> {
//            try {
//                writeValue(basePath, new Value("v", 8L));
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        });

        TimeUnit.SECONDS.sleep(1);
        latch.countDown();
    }

    private static final CountDownLatch latch = new CountDownLatch(1);

    private static void writeValue(String path, Value value) throws Exception {
        String threadName = Thread.currentThread().getName();
        Stat stat = CLIENT.checkExists().forPath(path);
        if (stat == null) {
            CLIENT.create().orSetData().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path,Long.toString(value.version()).getBytes());
            System.out.println(threadName + ": created");
            return;
        }

        long nodeValue = Long.parseLong(new String(CLIENT.getData().storingStatIn(stat).forPath(path)));
        while (nodeValue < value.version()) {
            try {
//                latch.await();
                Stat stat1 = CLIENT.setData().withVersion(stat.getVersion()).forPath(path, Long.toString(value.version()).getBytes());
                System.out.println(threadName + ": updated: " + stat1);
                break;
            } catch (KeeperException.BadVersionException e) {
                System.out.println(threadName + ": already published.");
                nodeValue = Long.parseLong(new String(CLIENT.getData().storingStatIn(stat).forPath(path)));
            }
        }
    }

    record Value(String value, long version) {}
}
