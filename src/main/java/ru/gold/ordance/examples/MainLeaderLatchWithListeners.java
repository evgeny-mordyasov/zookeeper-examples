package ru.gold.ordance.examples;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.UUID.randomUUID;
import static ru.gold.ordance.CuratorFrameworks.newAutostartClient;

public class MainLeaderLatchWithListeners {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainLeaderLatchWithListeners.class);

    private static volatile boolean isFinished;

    public static void main(String[] args) throws Exception {
        var client = newAutostartClient();
        var latchPath = "/data";

        LeaderLatch leader1 = createLeader(client, latchPath);
        LeaderLatch leader2 = createLeader(client, latchPath);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        leader1.addListener(new LoggingLeaderLatchListener(leader1.getId()), executor);
        leader2.addListener(new LoggingLeaderLatchListener(leader2.getId()), executor);

        leader1.start();
        leader2.start();

        loop(leader1);
        loop(leader2);

        TimeUnit.SECONDS.sleep(20);

        isFinished = true;

        while (leader1.getState().equals(LeaderLatch.State.STARTED) || leader2.getState().equals(LeaderLatch.State.STARTED)) {}
        executor.shutdown();
    }

    private static LeaderLatch createLeader(CuratorFramework client, String latchPath) {
        return new LeaderLatch(
                client,
                latchPath,
                "pod-name-" + randomUUID(),
                LeaderLatch.CloseMode.NOTIFY_LEADER);
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

    private record LoggingLeaderLatchListener(String podName) implements LeaderLatchListener {

        @Override
        public void isLeader() {
            LOGGER.info("{}: I am leader.", podName);
        }

        @Override
        public void notLeader() {
            LOGGER.info("{}: I am not leader.", podName);
        }
    }
}
