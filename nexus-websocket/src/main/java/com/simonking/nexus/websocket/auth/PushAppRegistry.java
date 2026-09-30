package com.simonking.nexus.websocket.auth;

import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.nexus.websocket.config.WsProperties;
import com.simonking.stream.nexus.common.constant.NexusConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 推送应用注册表：鉴权的唯一数据源
 *
 * <p>应用不在配置文件里维护：内置一个开箱即用的默认应用
 * {@value NexusConstants#DEFAULT_APP_ID} / {@value NexusConstants#DEFAULT_API_KEY}（白名单 {@code test}），
 * 它是**只读凭证**：始终在列表里，appId / apiKey / 白名单 都不可改、也不可删除
 * （改了等于把联调与文档的基准改掉，而且所有接入方都要跟着换）。
 * 其余应用在 {@code /admin} 管理页运行时增删。
 *
 * <p><b>内存表 + 版本号缓存</b>：鉴权是推送热路径，每次推送都读一次台账，
 * 不能每次都问存储。这里常驻一份内存表，靠 {@link PushAppStore#version()} 判断是否过期：
 * <ul>
 *     <li>{@code app-cache-ttl} 内的读操作完全不碰存储；</li>
 *     <li>写操作的节点会广播 {@code APPS_CHANGED}，其他节点收到后立即重载，不必等 TTL；</li>
 *     <li>存储抖动时只告警、继续用本地缓存——台账短暂陈旧好过推送整体不可用。</li>
 * </ul>
 *
 * <p>校验顺序不可颠倒：必须**先按 appId 定位应用**（确定归属白名单），再常量时间比对密钥。
 * 反过来做就变成了「遍历所有应用找一个密钥匹配的」，既慢又会泄漏信息。
 *
 * @author simonking
 */
@Slf4j
@Component
public class PushAppRegistry {

    /**
     * 内置默认应用的模块白名单：只放 {@code test}，避免它成为「能推任意模块的万能钥匙」
     *
     * <p>appId / apiKey 是与 SSE 服务共用的内置常量，收在 {@link NexusConstants}，不在这里重复定义。
     */
    public static final List<String> DEFAULT_ALLOWED_MODULES = List.of("test");

    private final boolean authEnabled;

    private final PushAppStore store;

    /**
     * 版本检查的最小间隔：防止每次推送都去问一次存储
     */
    private final long cacheTtlMillis;

    /**
     * 写路径互斥：save / remove / reload 三者不能交叉，否则会把旧快照覆盖回去
     */
    private final Object lock = new Object();

    /**
     * 按 appId 有序：管理页列表顺序稳定。整体替换引用而非原地清空，
     * 让读路径（无锁）永远看到一个完整快照，不会撞上「清空了一半」的中间态
     */
    private volatile ConcurrentMap<String, PushApp> apps = new ConcurrentSkipListMap<>();

    /**
     * 内存表对应的台账版本号：与 {@link PushAppStore#version()} 不同即需要重载
     */
    private volatile long loadedVersion = -1L;

    private volatile long lastVersionCheck;

    public PushAppRegistry(WsProperties properties, PushAppStore store, WsClusterProperties clusterProperties) {
        this.authEnabled = properties.isAuthEnabled();
        this.store = store;
        this.cacheTtlMillis = Math.max(clusterProperties.getAppCacheTtl().toMillis(), 0);
        synchronized (lock) {
            loadAll();
        }
        log.info("[ws] 推送应用载入完成：共 {} 个，台账 {}，鉴权开关 authEnabled={}",
                apps.size(), store.describe(), authEnabled);
    }

    /**
     * 生成随机密钥：UUID(16字节) → Base64 URL-safe 无补位 = 22 字符，放进请求头不必转义
     */
    public static String generateApiKey() {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = new byte[16];
        ByteBuffer.wrap(bytes).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * 模块白名单归一化：空 / 全空白一律视为 {@code *}（不限模块）
     */
    public static List<String> normalize(List<String> modules) {
        if (CollectionUtils.isEmpty(modules)) {
            return List.of(NexusConstants.MODULE_WILDCARD);
        }
        List<String> normalized = modules.stream()
                .filter(StringUtils::hasText)
                .map(String::trim)
                .distinct()
                .toList();
        return normalized.isEmpty() ? List.of(NexusConstants.MODULE_WILDCARD) : normalized;
    }

    /**
     * 内置默认应用：始终存在于台账，三个字段都不可改、整条不可删除（接口与页面都会拦，这里再兜一层）
     */
    public static boolean isBuiltIn(String appId) {
        return NexusConstants.DEFAULT_APP_ID.equals(appId == null ? null : appId.trim());
    }

    /**
     * 内置默认应用的固定形态（启动时校正、页面展示都以此为准）
     */
    public static PushApp defaultApp() {
        return new PushApp(NexusConstants.DEFAULT_APP_ID, NexusConstants.DEFAULT_API_KEY, DEFAULT_ALLOWED_MODULES);
    }

    public boolean isAuthEnabled() {
        return authEnabled;
    }

    /**
     * 台账位置（管理页展示用：让人知道改的东西写在哪）
     *
     * <p>单机是文件路径、集群是 Redis Hash——字段名不叫 storePath，因为集群下没有文件。
     */
    public String storeLocation() {
        return store.describe();
    }

    public List<PushApp> list() {
        refreshIfStale();
        return List.copyOf(apps.values());
    }

    /**
     * 按 appId 定位应用；未知 appId 返回 empty（鉴权侧据此判 401）
     */
    public Optional<PushApp> find(String appId) {
        if (!StringUtils.hasText(appId)) {
            return Optional.empty();
        }
        refreshIfStale();
        return Optional.ofNullable(apps.get(appId.trim()));
    }

    public boolean contains(String appId) {
        return find(appId).isPresent();
    }

    /**
     * 凭证校验：鉴权关闭时一律放行（此时白名单为 null，调用方不做模块越权校验）
     *
     * @return 校验通过返回应用，否则 null
     */
    public PushApp authenticate(String appId, String apiKey) {
        if (!authEnabled) {
            return null;
        }
        // 多节点下凭证可能刚被别的节点改过：先按 TTL 做一次版本检查，绝大多数情况只是一次时间比较
        refreshIfStale();
        PushApp app = apps.get(appId);
        if (app == null || !constantTimeEquals(app.apiKey(), apiKey)) {
            return null;
        }
        return app;
    }

    /**
     * 模块白名单校验：{@code allowed} 为 null（鉴权关闭）或含 {@code *} 时放行
     */
    public static boolean moduleAllowed(List<String> allowed, String bizModule) {
        if (allowed == null || !StringUtils.hasText(bizModule)) {
            return true;
        }
        if (allowed.contains(NexusConstants.MODULE_WILDCARD)) {
            return true;
        }
        return allowed.stream().anyMatch(m -> m.equals(bizModule));
    }

    /**
     * 新增或覆盖同名应用
     *
     * <p>先落存储再进内存：反过来会「内存有、存储没有」，其他节点永远看不到，
     * 而本节点还一直按新凭证放行。落存储失败直接抛——宁可这一次改不动，
     * 也不要让人以为存住了。
     *
     * <p><b>不在这里推进 {@link #loadedVersion}</b>：写入与取版本之间如果插进另一个节点的写入，
     * 会话「记下更高版本、手里却是旧数据」，之后除非再有写入，这个节点会一直停在旧台账。
     * 让版本标记故意落后一截，下一次检查就会从存储重读整张表（代价是一次 HGETALL，写操作本就极少）。
     */
    public void save(PushApp app) {
        synchronized (lock) {
            store.save(app);
            apps.put(app.appId(), app);
        }
    }

    /**
     * 删除应用：先删存储再删内存，删不动就保持原样
     *
     * @return 内存里是否真的有这一条（存储里没有也算删成功——本来就不在）
     */
    public boolean remove(String appId) {
        if (!StringUtils.hasText(appId)) {
            return false;
        }
        String key = appId.trim();
        if (isBuiltIn(key)) {
            log.warn("[ws] 内置默认应用 {} 不可删除，已忽略该请求", key);
            return false;
        }
        synchronized (lock) {
            if (!apps.containsKey(key)) {
                return false;
            }
            store.delete(key);
            apps.remove(key);
            return true;
        }
    }

    /**
     * 无条件重载（收到 {@code APPS_CHANGED} 时调用）
     *
     * <p>广播是低频事件，这里故意不先比版本号：写节点可能「吸收了」其他节点的版本递增
     * （HSET 与 INCR 分开两步，中间夹进另一个写者），此时版本号相同不代表内容相同。
     * 宁可多一次 HGETALL，也不要漏掉一条凭证变更。
     */
    public void reload() {
        synchronized (lock) {
            loadAll();
            log.info("[ws] 推送应用台账已重载：{} 条", apps.size());
        }
    }

    /**
     * 台账版本变了就重载（TTL 到期时调用，避免无改动时白读一次整表）
     *
     * @return 是否真的重载了
     */
    public boolean reloadIfChanged() {
        synchronized (lock) {
            if (store.version() == loadedVersion) {
                return false;
            }
            loadAll();
            log.info("[ws] 推送应用台账已重载：{} 条，版本 {}", apps.size(), loadedVersion);
            return true;
        }
    }

    /**
     * 按 TTL 节流的版本检查：读路径调用，绝大多数情况下只是一次时间比较
     *
     * <p>异常只告警：台账短暂陈旧好过让每次推送都因为存储抖动而失败。
     */
    private void refreshIfStale() {
        long now = System.currentTimeMillis();
        if (now - lastVersionCheck < cacheTtlMillis) {
            return;
        }
        lastVersionCheck = now;
        try {
            if (store.version() == loadedVersion) {
                return;
            }
            reloadIfChanged();
        } catch (Exception e) {
            log.warn("[ws] 推送应用台账刷新失败，继续使用本地缓存：{}", e.getMessage());
        }
    }

    /**
     * 读全量台账并校正内置默认应用（调用方必须持有 {@link #lock}）
     *
     * <p>校正必须做：台账被人工改过、或从旧版本升级上来时，内置凭证可能缺失或漂移，
     * 而它是文档与联调页的基准，必须恒等于代码里的常量。
     *
     * <p><b>先取版本再读数据</b>：{@link #loadedVersion} 的语义是「内存至少反映了这个版本」。
     * 反过来的窗口是「读完数据、取版本之间插进一次写入」——会记下更高的版本却拿着旧数据，
     * 且只要不再有新写入就永远不会自愈。多一次重载只是多发一次 HGETALL。
     */
    private void loadAll() {
        long versionBeforeLoad = store.version();

        Map<String, PushApp> fresh = new LinkedHashMap<>();
        store.load().forEach(app -> fresh.put(app.appId(), app));

        PushApp builtIn = fresh.get(NexusConstants.DEFAULT_APP_ID);
        boolean dirty = builtIn == null
                || !NexusConstants.DEFAULT_API_KEY.equals(builtIn.apiKey())
                || !DEFAULT_ALLOWED_MODULES.equals(builtIn.allowedModules());
        if (dirty) {
            fresh.put(NexusConstants.DEFAULT_APP_ID, defaultApp());
            store.save(defaultApp());
        }

        ConcurrentMap<String, PushApp> next = new ConcurrentSkipListMap<>();
        next.putAll(fresh);
        apps = next;
        loadedVersion = versionBeforeLoad;
    }

    /**
     * 常量时间比较，避免时序侧信道
     */
    public static boolean constantTimeEquals(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
    }
}
