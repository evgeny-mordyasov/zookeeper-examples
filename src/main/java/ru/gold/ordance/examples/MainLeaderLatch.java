package ru.gold.ordance.examples;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static java.util.UUID.randomUUID;
import static ru.gold.ordance.CuratorFrameworks.newAutostartClient;

public class MainLeaderLatch {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainLeaderLatch.class);

    private static volatile boolean isFinished;

    public static void main(String[] args) throws Exception {
        var client = newAutostartClient();
        var latchPath = "/data";

        LeaderLatch leader1 = createLeader(client, latchPath);
        LeaderLatch leader2 = createLeader(client, latchPath);

        leader1.start();
        leader2.start();

        loop(leader1);
        loop(leader2);

        TimeUnit.SECONDS.sleep(20);

        isFinished = true;
    }

    private static LeaderLatch createLeader(CuratorFramework client, String latchPath) {
        return new LeaderLatch(
                client,
                latchPath,
                "pod-name-" + randomUUID());
    }

    private static void loop(LeaderLatch leader) {
        new Thread(() -> {
            try {
                while (!isFinished) {
                    LOGGER.info("{} : {}", leader.getId(), leader.hasLeadership());
                    TimeUnit.SECONDS.sleep(1);
                }

                leader.close();
            } catch (Exception e) {
                LOGGER.error("Exception.", e);
            }
        }).start();
    }
}
