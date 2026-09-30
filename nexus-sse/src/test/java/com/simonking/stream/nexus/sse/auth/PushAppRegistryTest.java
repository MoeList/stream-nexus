package com.simonking.stream.nexus.sse.auth;

import com.simonking.stream.nexus.common.constant.NexusConstants;
import com.simonking.stream.nexus.sse.config.ClusterProperties;
import com.simonking.stream.nexus.sse.config.SseProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 应用台账缓存：决定「多节点改完多久生效」和「存储抖动时推送还能不能用」，两种边界都要覆盖
 *
 * @author simonking
 */
class PushAppRegistryTest {

    @Test
    void seedsBuiltInAppWhenStoreIsEmpty() {
        InMemoryStore store = new InMemoryStore();
        PushAppRegistry registry = registry(store, Duration.ofSeconds(30));

        // 内置默认应用必须始终在，且形态与代码常量一致：它是文档与联调页的基准
        PushApp builtIn = registry.find(NexusConstants.DEFAULT_APP_ID).orElse(null);
        assertNotNull(builtIn);
        assertEquals(NexusConstants.DEFAULT_API_KEY, builtIn.apiKey());
        assertEquals(PushAppRegistry.DEFAULT_ALLOWED_MODULES, builtIn.allowedModules());
        // 校正要落存储：只改内存的话，其他节点永远看不到
        assertEquals(1, store.apps.size());
    }

    @Test
    void reloadIfChangedOnlyWhenVersionAdvances() {
        InMemoryStore store = new InMemoryStore();
        PushAppRegistry registry = registry(store, Duration.ofSeconds(30));

        assertFalse(registry.reloadIfChanged(), "版本没变不该重载");
        assertFalse(registry.contains("order-service"));

        // 模拟「另一个节点」写入：数据 + 版本一起变
        store.putExternally(new PushApp("order-service", "k1", List.of("order")));
        assertTrue(registry.reloadIfChanged());
        assertTrue(registry.contains("order-service"));
        assertFalse(registry.reloadIfChanged(), "重载过之后版本已对齐");
    }

    @Test
    void readsWithinTtlDoNotTouchStore() {
        InMemoryStore store = new InMemoryStore();
        PushAppRegistry registry = registry(store, Duration.ofSeconds(30));

        // 第一次读一定会查一次版本（lastVersionCheck 初值为 0），之后落在这轮 TTL 内
        registry.find(NexusConstants.DEFAULT_APP_ID);
        int afterWarmUp = store.versionReads;
        for (int i = 0; i < 20; i++) {
            registry.list();
            registry.find(NexusConstants.DEFAULT_APP_ID);
        }
        assertEquals(afterWarmUp, store.versionReads, "TTL 内不该反复去查存储版本");
    }

    @Test
    void expiredTtlPicksUpChangeWithoutBroadcast() {
        InMemoryStore store = new InMemoryStore();
        // TTL=0：读路径每次都查版本，等价于「广播丢了，只能靠 TTL 到期兜底」
        PushAppRegistry registry = registry(store, Duration.ZERO);

        assertFalse(registry.contains("late-app"));
        store.putExternally(new PushApp("late-app", "k2", List.of("late")));
        assertTrue(registry.contains("late-app"), "TTL 到期后必须自己发现版本变化");
    }

    @Test
    void reloadAlwaysRereadsEvenWhenVersionUnchanged() {
        InMemoryStore store = new InMemoryStore();
        PushAppRegistry registry = registry(store, Duration.ofSeconds(30));

        // 模拟「版本号被并发的写入吸收」：数据变了、版本号没变
        store.putWithoutVersion(new PushApp("sneaky", "k3", List.of("sneaky")));
        assertFalse(registry.contains("sneaky"));

        registry.reload();
        assertTrue(registry.contains("sneaky"), "广播重载不能因为版本号相同就跳过");
    }

    @Test
    void builtInAppCannotBeRemoved() {
        InMemoryStore store = new InMemoryStore();
        PushAppRegistry registry = registry(store, Duration.ofSeconds(30));

        assertFalse(registry.remove(NexusConstants.DEFAULT_APP_ID));
        assertTrue(registry.contains(NexusConstants.DEFAULT_APP_ID));
    }

    private static PushAppRegistry registry(PushAppStore store, Duration cacheTtl) {
        ClusterProperties clusterProperties = new ClusterProperties();
        clusterProperties.setAppCacheTtl(cacheTtl);
        PushAppRegistry registry = new PushAppRegistry(new SseProperties(), store, clusterProperties);
        // 构造时若校正了内置应用（写过存储），版本标记会「故意落后一拍」，首次读会多一次重载。
        // 这里先对齐，让每个用例只观察自己制造的变化。
        registry.reload();
        return registry;
    }

    /**
     * 内存台账：用版本号模拟 Redis 的 {@code apps:version}，
     * 并统计 {@code version()} 调用次数来验证 TTL 节流真的生效
     */
    private static final class InMemoryStore implements PushAppStore {

        private final Map<String, PushApp> apps = new LinkedHashMap<>();

        private final AtomicLong version = new AtomicLong();

        private int versionReads;

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public List<PushApp> load() {
            return new ArrayList<>(apps.values());
        }

        @Override
        public void save(PushApp app) {
            apps.put(app.appId(), app);
            version.incrementAndGet();
        }

        @Override
        public boolean delete(String appId) {
            if (apps.remove(appId) == null) {
                return false;
            }
            version.incrementAndGet();
            return true;
        }

        @Override
        public long version() {
            versionReads++;
            return version.get();
        }

        @Override
        public String describe() {
            return "memory";
        }

        void putExternally(PushApp app) {
            save(app);
        }

        /**
         * 只改数据、不动版本：用来构造「版本号被吸收」的极端场景
         */
        void putWithoutVersion(PushApp app) {
            apps.put(app.appId(), app);
        }
    }
}
