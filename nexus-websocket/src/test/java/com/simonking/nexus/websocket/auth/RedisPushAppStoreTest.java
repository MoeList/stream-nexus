package com.simonking.nexus.websocket.auth;

import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.nexus.websocket.config.WsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Redis 台账：多节点共享同一份应用表，写入顺序与版本号语义错了会让「改了不生效」或「读到旧凭证」
 *
 * @author simonking
 */
class RedisPushAppStoreTest {

    private static final String APPS_KEY = "nexus:ws:apps";

    private static final String VERSION_KEY = "nexus:ws:apps:version";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);

    @SuppressWarnings("unchecked")
    private final HashOperations<String, Object, Object> hashOps = mock(HashOperations.class);

    private final ValueOperations<String, String> valueOps = mock(ValueOperations.class);

    @BeforeEach
    void wireRedis() {
        // 构造器会立即读一次版本号打日志，先把这两层依赖备好
        when(redis.opsForHash()).thenReturn(hashOps);
        when(redis.opsForValue()).thenReturn(valueOps);
        when(redis.hasKey(anyString())).thenReturn(false);
    }

    @Test
    void saveWritesDataBeforeBumpingVersion() throws Exception {
        RedisPushAppStore store = newStore(new WsClusterProperties(), new WsProperties());
        store.save(new PushApp("order-service", "k1", List.of("order")));

        // 顺序不能反：先递增版本会让其他节点「看到版本变了、读到的还是旧数据」
        InOrder inOrder = inOrder(hashOps, valueOps);
        inOrder.verify(hashOps).put(eq(APPS_KEY), eq("order-service"), anyString());
        inOrder.verify(valueOps).increment(VERSION_KEY);

        // 值必须能原样解析回来
        org.mockito.ArgumentCaptor<String> written = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(hashOps).put(eq(APPS_KEY), eq("order-service"), written.capture());
        assertEquals("order-service",
                mapper.readValue(written.getValue(), PushApp.class).appId());
    }

    @Test
    void loadParsesRecordsAndSkipsBrokenEntries() throws Exception {
        Map<Object, Object> raw = new LinkedHashMap<>();
        raw.put("b-app", mapper.writeValueAsString(new PushApp("b-app", "k2", List.of("order"))));
        raw.put("broken", "{not-json");
        raw.put("a-app", mapper.writeValueAsString(new PushApp("a-app", "k1", List.of("pay"))));
        when(hashOps.entries(APPS_KEY)).thenReturn(raw);

        List<PushApp> apps = newStore(new WsClusterProperties(), new WsProperties()).load();

        // 单条损坏跳过即可；顺序按 appId 排，管理页不会因 Hash 遍历顺序而抖动
        assertEquals(List.of("a-app", "b-app"), apps.stream().map(PushApp::appId).toList());
    }

    @Test
    void deleteBumpsVersionOnlyWhenSomethingRemoved() {
        RedisPushAppStore store = newStore(new WsClusterProperties(), new WsProperties());

        when(hashOps.delete(APPS_KEY, "gone")).thenReturn(0L);
        assertFalse(store.delete("gone"));
        verify(valueOps, never()).increment(anyString());

        when(hashOps.delete(APPS_KEY, "app-x")).thenReturn(1L);
        assertTrue(store.delete("app-x"));
        verify(valueOps).increment(VERSION_KEY);
    }

    @Test
    void versionFallsBackToZeroWhenMissingOrIllegal() {
        RedisPushAppStore store = newStore(new WsClusterProperties(), new WsProperties());

        when(valueOps.get(VERSION_KEY)).thenReturn(null);
        assertEquals(0L, store.version());

        // 被人工改坏：当作 0，下次写入重新从 1 开始，而不是让节点一直重载失败
        when(valueOps.get(VERSION_KEY)).thenReturn("not-a-number");
        assertEquals(0L, store.version());

        when(valueOps.get(VERSION_KEY)).thenReturn("42");
        assertEquals(42L, store.version());
    }

    @Test
    void existsMeansVersionKeyPresent() {
        RedisPushAppStore store = newStore(new WsClusterProperties(), new WsProperties());

        // 用户把应用删光后 Hash 为空但版本号仍在：不能被当成「未初始化」再播一次默认应用
        when(redis.hasKey(VERSION_KEY)).thenReturn(true);
        assertTrue(store.exists());
        assertTrue(store.describe().contains(APPS_KEY));
    }

    @Test
    void importsLocalFileWhenEnabled(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("push-apps.json");
        Files.writeString(file, mapper.writeValueAsString(
                List.of(new PushApp("legacy", "k", List.of("order")))));

        WsProperties ws = new WsProperties();
        ws.setAppStorePath(file.toString());
        WsClusterProperties cluster = new WsClusterProperties();
        cluster.setAppsImportOnStartup(true);

        newStore(cluster, ws);

        // 首次迁移：单机时代的本地台账要整份搬进 Redis，并打上版本号
        verify(hashOps).put(eq(APPS_KEY), eq("legacy"), anyString());
        verify(valueOps).increment(VERSION_KEY);
    }

    @Test
    void skipsImportWhenRedisAlreadyInitialized() {
        when(redis.hasKey(VERSION_KEY)).thenReturn(true);

        WsProperties ws = new WsProperties();
        ws.setAppStorePath("data/does-not-exist.json");
        WsClusterProperties cluster = new WsClusterProperties();
        cluster.setAppsImportOnStartup(true);

        newStore(cluster, ws);

        // 已有台账再导入会覆盖多节点共享的数据，必须跳过
        verify(hashOps, never()).put(anyString(), anyString(), anyString());
    }

    private RedisPushAppStore newStore(WsClusterProperties cluster, WsProperties ws) {
        return new RedisPushAppStore(redis, cluster, ws, mapper);
    }
}
