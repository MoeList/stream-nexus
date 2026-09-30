package com.simonking.stream.nexus.sse.controller;

import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.cluster.ClusterMessageType;
import com.simonking.stream.nexus.common.cluster.ClusterNodeInfo;
import com.simonking.stream.nexus.common.constant.NexusConstants;
import com.simonking.stream.nexus.sse.admin.ConnectionViewMapper;
import com.simonking.stream.nexus.sse.auth.PushApp;
import com.simonking.stream.nexus.sse.auth.PushAppRegistry;
import com.simonking.stream.nexus.sse.cluster.ClusterBus;
import com.simonking.stream.nexus.sse.cluster.ClusterNodeIdentity;
import com.simonking.stream.nexus.sse.cluster.ClusterNodeRegistry;
import com.simonking.stream.nexus.sse.cluster.ClusterSnapshotCollector;
import com.simonking.stream.nexus.sse.config.ClusterProperties;
import com.simonking.stream.nexus.sse.config.SseProperties;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 连接运维接口（供 {@code /admin.html} 管理页使用）
 *
 * <p>只做两件事：看全量连接的实时状态、强制下线某条连接。
 *
 * @author simonking
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/sse/admin")
public class AdminController {

    /**
     * 进程启动时刻：运维关心的「项目跑了多久」从这一刻算起。
     *
     * <p>取 JVM 启动时间而非 Bean 初始化时间——进程起来的那一刻就已经占着端口了，
     * 用它最接近运维体感；静态常量避免每次请求都去问一次 MXBean。
     */
    private static final long START_TIME = ManagementFactory.getRuntimeMXBean().getStartTime();

    private final SseClientRegistry registry;

    private final PushAppRegistry appRegistry;

    private final SseProperties properties;

    private final ConnectionViewMapper viewMapper;

    private final ObjectProvider<ClusterBus> clusterBusProvider;

    private final ClusterNodeIdentity identity;

    private final ClusterProperties clusterProperties;

    private final ObjectProvider<ClusterNodeRegistry> nodeRegistryProvider;

    /**
     * 集群模式的连接快照收集器；单机没有这个 Bean，getIfAvailable() 返回 null
     */
    private final ObjectProvider<ClusterSnapshotCollector> snapshotCollectorProvider;

    /**
     * 推送应用列表（管理页的应用台账 / 测试页的下拉数据源）
     *
     * <p>返回明文 apiKey 是有意为之：测试页要靠它拼鉴权头。
     * 代价是「拿到这个接口就拿到全部推送权限」，见文档 E15——该接口必须内网隔离。
     */
    @GetMapping("/apps")
    public Map<String, Object> apps() {
        List<PushApp> items = appRegistry.list();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", items.size());
        result.put("authEnabled", appRegistry.isAuthEnabled());
        result.put("defaultAppId", NexusConstants.DEFAULT_APP_ID);
        // 台账位置：单机是文件路径、集群是 Redis Hash，页面直接展示（字段名不再叫 storePath，因为集群下没有文件）
        result.put("storeLocation", appRegistry.storeLocation());
        result.put("items", items);
        return result;
    }

    /**
     * 生成一个随机 apiKey（GUID → Base64），供管理页「自动生成」按钮使用
     */
    @GetMapping("/apps/generate-key")
    public Map<String, Object> generateKey() {
        return Map.of("apiKey", PushAppRegistry.generateApiKey());
    }

    /**
     * 新增应用：同名已存在时拒绝（避免手滑覆盖线上凭证，覆盖请先删除）
     *
     * <p>{@code apiKey} 留空则由服务端生成（GUID → Base64），避免人工起弱口令。
     */
    @PostMapping("/apps")
    public Map<String, Object> addApp(@RequestBody AppRequest body) {
        if (body == null || !StringUtils.hasText(body.appId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "appId is required");
        }
        String appId = body.appId().trim();
        if (appRegistry.contains(appId)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "appId already exists: " + appId);
        }
        String apiKey = StringUtils.hasText(body.apiKey()) ? body.apiKey().trim() : PushAppRegistry.generateApiKey();
        appRegistry.save(new PushApp(appId, apiKey, PushAppRegistry.normalize(body.allowedModules())));
        broadcastAppsChanged();
        return Map.of("ok", true, "appId", appId, "apiKey", apiKey);
    }

    /**
     * 修改应用：只改传了的字段，{@code appId} 本身不可改（改标识请删除后重建）
     *
     * <p>{@code apiKey} 为空 / 全空白表示不修改；{@code allowedModules} 为 null 表示不修改，
     * 传空数组表示清空限制（归一化成 {@code *}）。改动立即生效：下一次推送就按新白名单鉴权。
     *
     * <p>内置默认应用例外：整条只读，appId / apiKey / 白名单 都改不动（有改动 → 409）；
     * 传回来的值与现状完全一致则视为「没改」，正常返回（幂等，不报错）。
     */
    @PutMapping("/apps/{appId}")
    public Map<String, Object> updateApp(@PathVariable String appId, @RequestBody AppUpdateRequest body) {
        PushApp existed = appRegistry.find(appId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "appId not found: " + appId));
        if (body == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "request body is required");
        }
        // 内置默认应用只读：键和值都写死在代码里，改了等于把联调与文档的基准改掉，
        // 且所有接入方都要跟着换——要别的凭证就新建应用
        if (PushAppRegistry.isBuiltIn(appId)) {
            boolean keyUnchanged = !StringUtils.hasText(body.apiKey())
                    || NexusConstants.DEFAULT_API_KEY.equals(body.apiKey().trim());
            boolean modulesUnchanged = body.allowedModules() == null
                    || PushAppRegistry.normalize(body.allowedModules()).equals(PushAppRegistry.DEFAULT_ALLOWED_MODULES);
            if (!keyUnchanged || !modulesUnchanged) {
                throw new ResponseStatusException(HttpStatus.CONFLICT,
                        "内置默认应用只读，appId / apiKey / 白名单 均不可修改：" + NexusConstants.DEFAULT_APP_ID);
            }
            Map<String, Object> unchanged = new LinkedHashMap<>();
            unchanged.put("ok", true);
            unchanged.put("appId", existed.appId());
            unchanged.put("apiKey", existed.apiKey());
            unchanged.put("allowedModules", existed.allowedModules());
            unchanged.put("changed", false);
            return unchanged;
        }
        String apiKey = StringUtils.hasText(body.apiKey()) ? body.apiKey().trim() : existed.apiKey();
        List<String> modules = body.allowedModules() == null
                ? existed.allowedModules()
                : PushAppRegistry.normalize(body.allowedModules());

        PushApp updated = new PushApp(existed.appId(), apiKey, modules);
        appRegistry.save(updated);
        broadcastAppsChanged();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("appId", updated.appId());
        result.put("apiKey", updated.apiKey());
        result.put("allowedModules", updated.allowedModules());
        return result;
    }

    /**
     * 删除应用：立即生效，该 appId 之后的推送一律 401；并同步落盘，重启不会自己回来
     *
     * <p>内置默认应用例外：始终存在且只读，删它返回 409（页面也不给删除 / 修改按钮）。
     * 要别的凭证就新建应用，而不是改它、删它。
     */
    @DeleteMapping("/apps/{appId}")
    public Map<String, Object> removeApp(@PathVariable String appId) {
        if (PushAppRegistry.isBuiltIn(appId)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "内置默认应用不可删除（只读凭证）：" + NexusConstants.DEFAULT_APP_ID);
        }
        boolean removed = appRegistry.remove(appId);
        if (removed) {
            broadcastAppsChanged();
        }
        return Map.of("ok", true, "appId", appId, "removed", removed);
    }

    /**
     * 连接与业务模块概览
     *
     * <p>集群模式下向各在线节点要一份快照并拼成一张表（{@code admin-timeout} 内没集齐就标
     * {@code partial}）。单机模式同一套字段口径，只是 {@code cluster=false}。
     *
     * <p>额外下发 {@code serverTime} 与 {@code heartbeatTimeout}：管理页据此算「静默时长」和
     * 「疑似失联」，避免依赖浏览器本机时钟（与服务端有偏差时心跳判读会失真）。
     *
     * <p>{@code startTime} / {@code uptime} 供台账展示「项目已运行多久」：同理用服务端时钟算，
     * 页面只负责格式化。
     */
    @GetMapping("/connections")
    public Map<String, Object> connections() {
        long now = System.currentTimeMillis();
        ClusterSnapshotCollector collector = snapshotCollectorProvider.getIfAvailable();
        ClusterSnapshotCollector.Snapshot snapshot = collector != null
                ? collector.collect(now)
                : standaloneSnapshot(now);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", snapshot.items().size());
        result.put("modules", moduleStats(snapshot.items()));
        result.put("serverTime", now);
        result.put("startTime", START_TIME);
        result.put("uptime", Math.max(now - START_TIME, 0));
        result.put("heartbeatTimeout", properties.getHeartbeatTimeout().toMillis());
        // 页面据此决定「节点」列与 partial 提示怎么显示；单机也下发 nodeId，两种模式渲染逻辑统一
        result.put("cluster", collector != null);
        result.put("nodeId", identity.getNodeId());
        result.put("partial", snapshot.partial());
        result.put("expectedNodes", snapshot.expectedNodes());
        result.put("receivedNodes", snapshot.receivedNodes());
        result.put("missingNodes", snapshot.missingNodes());
        result.put("items", snapshot.items());
        return result;
    }

    /**
     * 单机视图：只有本进程一份，不存在「没集齐」
     */
    private ClusterSnapshotCollector.Snapshot standaloneSnapshot(long now) {
        List<Map<String, Object>> items = registry.all().stream()
                .map(client -> viewMapper.toView(client, now))
                .toList();
        return new ClusterSnapshotCollector.Snapshot(items, 1, 1, List.of(), false);
    }

    /**
     * 集群节点视图
     *
     * <p>单机模式同样返回一条记录（就是本进程），页面不用为两种模式写两套渲染：
     * {@code cluster=false} 只影响「运行模式」那一格的文案。
     *
     * <p>{@code online} 的判定：本节点永远算在线（即便刚启动、注册信息还没写完），
     * 其他节点按 {@code node-timeout} 判断，与 {@link ClusterNodeRegistry} 保持一致。
     */
    @GetMapping("/cluster/nodes")
    public Map<String, Object> clusterNodes() {
        long now = System.currentTimeMillis();
        long timeout = clusterProperties.getNodeTimeout().toMillis();
        ClusterNodeRegistry nodeRegistry = nodeRegistryProvider.getIfAvailable();

        List<ClusterNodeInfo> nodes;
        if (nodeRegistry != null) {
            nodes = nodeRegistry.nodes();
        } else {
            nodes = List.of(ClusterNodeInfo.builder()
                    .nodeId(identity.getNodeId())
                    .nodeSeq(identity.getNodeSeq())
                    .instanceId(identity.getInstanceId())
                    .address(identity.getAddress())
                    .startTime(identity.getStartTime())
                    .lastSeen(now)
                    .clientCount(registry.size())
                    .modules(registry.moduleStats())
                    .build());
        }

        List<Map<String, Object>> items = new ArrayList<>(nodes.size());
        int online = 0;
        for (ClusterNodeInfo node : nodes) {
            boolean self = identity.getNodeId().equals(node.getNodeId());
            boolean alive = self || now - node.getLastSeen() <= timeout;
            if (alive) {
                online++;
            }
            items.add(toNodeView(node, now, self, alive));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("cluster", nodeRegistry != null);
        result.put("nodeId", identity.getNodeId());
        result.put("nodeSeq", identity.getNodeSeq());
        result.put("address", identity.getAddress());
        result.put("instanceId", identity.getInstanceId());
        result.put("nodeTimeout", timeout);
        result.put("nodeHeartbeatInterval", clusterProperties.getNodeHeartbeatInterval().toMillis());
        result.put("serverTime", now);
        result.put("online", online);
        result.put("total", items.size());
        result.put("items", items);
        return result;
    }

    /**
     * 强制下线：走统一回收入口，连接主表与模块索引一起清理
     *
     * <p>{@code clientId} 是服务端建连时分配的，管理页从 {@link #connections()} 的台账里取，
     * 不需要（也无法）由调用方自己构造。
     */
    @DeleteMapping("/connections/{clientId}")
    public Map<String, Object> kick(@PathVariable String clientId) {
        registry.remove(clientId);
        boolean published = publishControl(ClusterMessage.builder()
                .type(ClusterMessageType.KICK)
                .originNodeId(identity.getNodeId())
                .clientId(clientId)
                .ts(System.currentTimeMillis())
                .build());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("clientId", clientId);
        result.put("published", published);
        return result;
    }

    /**
     * 全部下线：一次清空所有连接
     *
     * <p>注意语义：客户端（浏览器 {@code EventSource}）断开后会自动重连，所以这是
     * <b>强制重连</b>而不是「封禁」——要真正拦住某类客户端请用建连鉴权（{@code connect-auth-enabled}）。
     * 典型用途是发布后让所有订阅端重新拉全量状态，或清掉一批僵尸连接。
     *
     * <p>返回实际下线条数（并发下可能略少于调用瞬间的台账数），页面据此回显。
     */
    @DeleteMapping("/connections")
    public Map<String, Object> kickAll() {
        int removed = registry.removeAll();
        boolean published = publishControl(ClusterMessage.builder()
                .type(ClusterMessageType.KICK_ALL)
                .originNodeId(identity.getNodeId())
                .ts(System.currentTimeMillis())
                .build());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("removed", removed);
        result.put("published", published);
        return result;
    }

    /**
     * 集群模式广播控制指令；单机没有 ClusterBus Bean，返回 false（本地操作已完成）
     */
    private boolean publishControl(ClusterMessage message) {
        ClusterBus bus = clusterBusProvider.getIfAvailable();
        if (bus == null) {
            return false;
        }
        try {
            bus.publishControl(message);
            return true;
        } catch (Exception e) {
            log.warn("[sse] 集群控制指令发布失败：type={}，{}", message.getType(), e.getMessage());
            return false;
        }
    }

    /**
     * 通知其他节点重载应用台账
     *
     * <p>写节点自己已经在 {@link PushAppRegistry#save} 里更新完内存，广播只是让其他节点的缓存
     * 立即失效，把「改完要等 app-cache-ttl」缩短到一次广播的延迟。广播失败不回滚写入：
     * 台账已经落存储，其他节点最迟 TTL 到期后照样对齐。
     */
    private void broadcastAppsChanged() {
        publishControl(ClusterMessage.builder()
                .type(ClusterMessageType.APPS_CHANGED)
                .originNodeId(identity.getNodeId())
                .ts(System.currentTimeMillis())
                .build());
    }

    /**
     * 按模块统计连接数（与 {@code SseClientRegistry.moduleStats()} 同口径：一条连接订阅几个模块就算几次）
     *
     * <p>统一从 items 算而不是本地注册表算：聚合时 items 已经包含了其他节点的连接，
     * 两条路径用同一个公式，页面上的模块标签才永远和表里的行对得上。
     *
     * <p>用 TreeMap 排序：并发遍历顺序不稳定，标签顺序每 5 秒跳一次会让人以为数据在变。
     */
    private static Map<String, Integer> moduleStats(List<Map<String, Object>> items) {
        Map<String, Integer> stats = new TreeMap<>();
        for (Map<String, Object> item : items) {
            if (!(item.get("modules") instanceof Collection<?> modules)) {
                continue;
            }
            for (Object module : modules) {
                stats.merge(String.valueOf(module), 1, Integer::sum);
            }
        }
        return stats;
    }

    /**
     * 新增应用的请求体：{@code allowedModules} 留空 = 不限制模块
     */
    public record AppRequest(String appId, String apiKey, List<String> allowedModules) {
    }

    /**
     * 修改应用的请求体：字段为 null / 空串表示「不改」，{@code allowedModules} 空数组表示不限模块
     */
    public record AppUpdateRequest(String apiKey, List<String> allowedModules) {
    }

    private Map<String, Object> toNodeView(ClusterNodeInfo node, long now, boolean self, boolean alive) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("nodeId", node.getNodeId());
        view.put("nodeSeq", node.getNodeSeq());
        view.put("instanceId", node.getInstanceId());
        view.put("address", node.getAddress());
        view.put("clientCount", node.getClientCount());
        view.put("modules", node.getModules() == null ? Map.<String, Integer>of() : node.getModules());
        view.put("startTime", node.getStartTime());
        view.put("lastSeen", node.getLastSeen());
        view.put("silence", Math.max(now - node.getLastSeen(), 0));
        view.put("online", alive);
        view.put("self", self);
        view.put("version", node.getVersion());
        return view;
    }
}
