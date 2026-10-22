package com.simonking.nexus.websocket.auth;

import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.nexus.websocket.config.WsProperties;
import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Redis 台账：多节点共享同一份推送应用（集群模式）
 *
 * <p>存储结构：
 * <ul>
 *     <li>{@code {ns}:apps}（Hash）：field = appId，value = {@link PushApp} JSON；</li>
 *     <li>{@code {ns}:apps:version}（String）：每次写操作 {@code INCR}，供各节点判断缓存是否过期。</li>
 * </ul>
 *
 * <p><b>版本号同时充当「已初始化」标记</b>：用户把应用删光后 Hash 会是空，
 * 若用「Hash 是否存在」判断首次启动，下次重启就会把内置默认应用又塞回来。
 * 而版本号只在写入时产生，删光应用并不会删它，语义正好。
 *
 * <p><b>不用 Lua 保证「数据 + 版本」原子</b>：Redis Cluster 下两个 Key 落在不同 slot，
 * 脚本会直接 {@code CROSSSLOT} 报错。这里选择「先写数据、再递增版本」的两步写法，
 * 中间失败的代价只是其他节点晚 {@code app-cache-ttl} 才看到新数据，不会读到脏数据。
 *
 * <p><b>首次迁移</b>：{@code apps-import-on-startup=true} 且 Redis 里还没有台账时，
 * 把本地 {@code push-apps.json} 导进来。只该在从单机切到集群时打开一次——
 * 多个节点同时开会互相覆盖。
 *
 * @author simonking
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "true")
public class RedisPushAppStore implements PushAppStore {

    private static final TypeReference<List<PushApp>> APP_LIST = new TypeReference<>() {
    };

    private final StringRedisTemplate redis;

    private final ObjectMapper objectMapper;

    private final String appsKey;

    private final String versionKey;

    public RedisPushAppStore(StringRedisTemplate redis,
                             WsClusterProperties clusterProperties,
                             WsProperties wsProperties,
                             ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.appsKey = ClusterConstants.keyApps(clusterProperties.getKeyPrefix());
        this.versionKey = ClusterConstants.keyAppsVersion(clusterProperties.getKeyPrefix());

        if (clusterProperties.isAppsImportOnStartup() && !exists()) {
            importFromFile(wsProperties.getAppStorePath());
        }
        log.info("[ws] 推送应用台账使用 Redis：{}（版本 {}）", appsKey, version());
    }

    @Override
    public boolean exists() {
        return Boolean.TRUE.equals(redis.hasKey(versionKey));
    }

    @Override
    public List<PushApp> load() {
        Map<Object, Object> raw = redis.opsForHash().entries(appsKey);
        List<PushApp> result = new ArrayList<>(raw.size());
        for (Map.Entry<Object, Object> entry : raw.entrySet()) {
            try {
                result.add(objectMapper.readValue(String.valueOf(entry.getValue()), PushApp.class));
            } catch (Exception e) {
                // 单条损坏不该拖垮整张台账：跳过并告警，管理员在页面上删掉重加即可
                log.warn("[ws] 推送应用记录解析失败，已跳过：appId={}, value={}", entry.getKey(), entry.getValue());
            }
        }
        // 按 appId 排序：多节点读到的顺序一致，管理页列表不会因 Hash 遍历顺序而抖动
        result.sort(Comparator.comparing(PushApp::appId));
        return result;
    }

    @Override
    public void save(PushApp app) {
        redis.opsForHash().put(appsKey, app.appId(), objectMapper.writeValueAsString(app));
        bumpVersion();
    }

    @Override
    public boolean delete(String appId) {
        Long removed = redis.opsForHash().delete(appsKey, appId);
        if (removed != null && removed > 0) {
            bumpVersion();
            return true;
        }
        return false;
    }

    @Override
    public long version() {
        String raw = redis.opsForValue().get(versionKey);
        if (raw == null) {
            return 0L;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            // 被人工改坏了：当作 0，下次写入会重新从 1 开始
            log.warn("[ws] 应用台账版本号非法，按 0 处理：{}", raw);
            return 0L;
        }
    }

    @Override
    public String describe() {
        return "Redis Hash " + appsKey + "（多节点共享）";
    }

    /**
     * 递增版本号：写在数据之后，让其他节点在「数据已可见」之后才收到刷新信号
     */
    private void bumpVersion() {
        redis.opsForValue().increment(versionKey);
    }

    /**
     * 把单机时代的本地 JSON 台账导入 Redis（仅首次迁移用）
     *
     * <p>读失败只告警不抛：导入是便利功能，失败后管理员仍可在页面重建台账，
     * 不该因此让节点起不来。
     */
    private void importFromFile(String configuredPath) {
        Path file = FilePushAppStore.resolve(configuredPath);
        if (!Files.isRegularFile(file)) {
            log.info("[ws] 未找到本地台账文件，跳过导入：{}", file);
            return;
        }
        try {
            String json = Files.readString(file, StandardCharsets.UTF_8);
            List<PushApp> apps = objectMapper.readValue(json, APP_LIST);
            int count = 0;
            for (PushApp app : apps) {
                redis.opsForHash().put(appsKey, app.appId(), objectMapper.writeValueAsString(app));
                count++;
            }
            // 即使文件里一条都没有也要打上版本号：否则下次启动会反复导入
            bumpVersion();
            log.info("[ws] 已把本地台账导入 Redis：{} 条（来源 {}）", count, file);
        } catch (Exception e) {
            log.warn("[ws] 本地台账导入 Redis 失败（可在管理页重建）：{}，{}", file, e.getMessage());
        }
    }
}
