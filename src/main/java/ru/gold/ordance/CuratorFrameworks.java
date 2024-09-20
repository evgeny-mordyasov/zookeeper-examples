package ru.gold.ordance;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public final class CuratorFrameworks {

    private static final String CONNECT_STRING = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private CuratorFrameworks() {
    }

    public static CuratorFramework newClient() {
        return CuratorFrameworkFactory.builder()
                .connectString(CONNECT_STRING)
                .retryPolicy(new ExponentialBackoffRetry(1000, 2))
                .build();
    }

    public static CuratorFramework newAutostartClient() {
        var client = newClient();
        client.start();
        return client;
    }
}
