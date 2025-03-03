package ru.gold.ordance.examples;

import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static ru.gold.ordance.CuratorFrameworks.newAutostartClient;

public class MainCuratorCache {

    public static void main(String[] args) throws Exception {
        var client = newAutostartClient();
        var basePath = "/base";
        client.create().idempotent().forPath(basePath);

        CuratorCache cache = CuratorCache.build(client, basePath);
        cache.listenable().addListener(
                CuratorCacheListener.builder()
                        .forCreatesAndChanges((oldNode, node) -> System.out.println(oldNode + "; " + node))
                        .build());
        cache.start();

        client.create().forPath("/base/abc", "data".getBytes(StandardCharsets.UTF_8));

        TimeUnit.SECONDS.sleep(10);
    }
}
