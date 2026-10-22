package com.simonking.nexus.websocket.controller;

import com.simonking.nexus.websocket.admin.ConnectionViewMapper;
import com.simonking.nexus.websocket.auth.PushApp;
import com.simonking.nexus.websocket.auth.PushAppRegistry;
import com.simonking.nexus.websocket.cluster.WsClusterBus;
import com.simonking.nexus.websocket.cluster.WsClusterNodeIdentity;
import com.simonking.nexus.websocket.cluster.WsClusterNodeRegistry;
import com.simonking.nexus.websocket.cluster.WsClusterSnapshotCollector;
import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.nexus.websocket.config.WsProperties;
import com.simonking.nexus.websocket.model.TcpClient;
import com.simonking.nexus.websocket.model.WsClient;
import com.simonking.nexus.websocket.registry.TcpClientRegistry;
import com.simonking.nexus.websocket.registry.WsClientRegistry;
import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.cluster.ClusterMessageType;
import com.simonking.stream.nexus.common.cluster.ClusterNodeInfo;
import com.simonking.stream.nexus.common.constant.NexusConstants;
import com.simonking.stream.nexus.common.constant.WsConstants;
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
 * 运维接口：连接台账 + 应用管理
 *
 * <p>与 SSE 模块保持同一套交互契约（{@code /ws/admin/**}），管理界面可无缝复用同一套心智。
 *
 * <p>集群模式下连接台账来自 {@link WsClusterSnapshotCollector} 的全集群快照（{@code admin-timeout}
 * 内没集齐就标 {@code partial}）；单机模式同一套字段口径，只是 {@code cluster=false}。
 *
 * @author simonking
 */
@Slf4j
@RestController
@RequestMapping("/ws/admin")
@RequiredArgsConstructor
public class AdminController {

    private static final long START_TIME = ManagementFactory.getRuntimeMXBean().getStartTime();

    private final WsClientRegistry registry;

    private final TcpClientRegistry tcpRegistry;

    private final PushAppRegistry appRegistry;

    private final WsProperties properties;

    private final ConnectionViewMapper viewMapper;

    private final ObjectProvider<WsClusterBus> clusterBusProvider;

    /**
     * 节点身份：单机模式也会初始化，管理页两种模式都能显示 nodeId
     */
    private final WsClusterNodeIdentity identity;

    private final WsClusterProperties clusterProperties;

    private final ObjectProvider<WsClusterNodeRegistry> nodeRegistryProvider;

    /**
     * 集群模式的连接快照收集器；单机没有这个 Bean，getIfAvailable() 返回 null
     */
    private final ObjectProvider<WsClusterSnapshotCollector> snapshotCollectorProvider;

    /**
     * 全局概览：WebSocket 连接 + 服务运行信息
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
        WsClusterSnapshotCollector collector = snapshotCollectorProvider.getIfAvailable();
        WsClusterSnapshotCollector.Snapshot snapshot = collector != null
                ? collector.collect(now)
                : standaloneSnapshot(now);
        // 快照里 WS 与 TCP 是同一批（省一次 Redis 往返），按 protocol 分流到两张表
        List<Map<String, Object>> items = filterProtocol(snapshot.items(), ConnectionViewMapper.PROTOCOL_WS);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", items.size());
        result.put("modules", moduleStats(items));
        result.put("serverTime", now);
        result.put("startTime", START_TIME);
        result.put("uptime", Math.max(now - START_TIME, 0));
        result.put("heartbeatInterval", properties.getHeartbeatInterval().toMillis());
        result.put("heartbeatTimeout", properties.getHeartbeatTimeout().toMillis());
        result.put("maxConnections", properties.getMaxConnections());
        result.put("wsPort", properties.getWsPort());
        result.put("wsPath", properties.getWsPath());
        // 协议标识 + TCP 侧的口径：概览要给出「WS / TCP」构成，页面不必为看一眼再拉一次 TCP 接口
        result.put("protocol", ConnectionViewMapper.PROTOCOL_WS);
        result.put("tcpPort", properties.getTcpPort());
        result.put("tcpTotal", countProtocol(snapshot.items(), ConnectionViewMapper.PROTOCOL_TCP));
        // 页面据此决定「节点」列与 partial 提示怎么显示；单机也下发 nodeId，两种模式渲染逻辑统一
        result.put("cluster", collector != null);
        result.put("nodeId", identity.getNodeId());
        result.put("partial", snapshot.partial());
        result.put("expectedNodes", snapshot.expectedNodes());
        result.put("receivedNodes", snapshot.receivedNodes());
        result.put("missingNodes", snapshot.missingNodes());
        result.put("items", items);
        return result;
    }

    /**
     * TCP 接入连接台账：哪些业务系统正连着、推了多少条
     *
     * <p>与终端台账分开：两者数量级差三个数量级，混在一张表里看不清，
     * 而且「连接数异常增长」的告警含义完全不同（终端涨是正常流量，TCP 涨多半是客户端重连逻辑写错）。
     *
     * <p>集群模式下同样走快照：业务系统连的是某一台的 TCP 端口，排障时得知道连在哪台。
     */
    @GetMapping("/tcp/connections")
    public Map<String, Object> tcpConnections() {
        long now = System.currentTimeMillis();
        WsClusterSnapshotCollector collector = snapshotCollectorProvider.getIfAvailable();
        WsClusterSnapshotCollector.Snapshot snapshot = collector != null
                ? collector.collect(now)
                : standaloneSnapshot(now);
        List<Map<String, Object>> items = filterProtocol(snapshot.items(), ConnectionViewMapper.PROTOCOL_TCP);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("protocol", ConnectionViewMapper.PROTOCOL_TCP);
        result.put("serverTime", now);
        result.put("total", items.size());
        result.put("tcpPort", properties.getTcpPort());
        result.put("maxConnections", properties.getMaxConnections());
        result.put("heartbeatInterval", properties.getHeartbeatInterval().toMillis());
        result.put("heartbeatTimeout", properties.getHeartbeatTimeout().toMillis());
        result.put("cluster", collector != null);
        result.put("nodeId", identity.getNodeId());
        result.put("partial", snapshot.partial());
        result.put("expectedNodes", snapshot.expectedNodes());
        result.put("receivedNodes", snapshot.receivedNodes());
        result.put("missingNodes", snapshot.missingNodes());
        result.put("items", items);
        return result;
    }

    /**
     * 单机视图：只有本进程一份，不存在「没集齐」
     *
     * <p>WS 与 TCP 一起返回，与集群快照保持同一种数据结构，上层分流逻辑只有一份。
     */
    private WsClusterSnapshotCollector.Snapshot standaloneSnapshot(long now) {
        List<Map<String, Object>> items = new ArrayList<>(registry.size() + tcpRegistry.size());
        for (WsClient client : registry.all()) {
            items.add(viewMapper.toView(client, now));
        }
        for (TcpClient client : tcpRegistry.all()) {
            items.add(viewMapper.toTcpView(client, now));
        }
        return new WsClusterSnapshotCollector.Snapshot(items, 1, 1, List.of(), false);
    }

    /**
     * 集群节点视图
     *
     * <p>单机模式同样返回一条记录（就是本进程），页面不用为两种模式写两套渲染：
     * {@code cluster=false} 只影响「运行模式」那一格的文案。
     *
     * <p>{@code online} 的判定：本节点永远算在线（即便刚启动、注册信息还没写完），
     * 其他节点按 {@code node-timeout} 判断，与 {@link WsClusterNodeRegistry} 保持一致。
     */
    @GetMapping("/cluster/nodes")
    public Map<String, Object> clusterNodes() {
        long now = System.currentTimeMillis();
        long timeout = clusterProperties.getNodeTimeout().toMillis();
        WsClusterNodeRegistry nodeRegistry = nodeRegistryProvider.getIfAvailable();

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
     * 强制下线指定 WebSocket 连接
     *
     * <p>本地先踢（本节点持有的连接立即关闭），再向全集群广播 KICK：
     * 管理页可能在任意节点上，连接只在其中一台手里。
     *
     * <p>{@code action} 承载踢下线原因，终端能从关闭帧里看到「谁踢的、为什么」。
     */
    @DeleteMapping("/connections/{clientId}")
    public Map<String, Object> kick(@PathVariable String clientId) {
        String reason = WsConstants.ACTION_KICKED + " by admin";
        registry.kick(clientId, reason);
        boolean published = publishControl(ClusterMessage.builder()
                .type(ClusterMessageType.KICK)
                .originNodeId(identity.getNodeId())
                .clientId(clientId)
                .action(reason)
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
     * <p>注意语义：客户端（浏览器）断开后会自动重连，所以这是<b>强制重连</b>而不是「封禁」——
     * 要真正拦住某类客户端请用建连鉴权（{@code connect-auth-enabled}）。
     * 典型用途是发布后让所有订阅端重新拉全量状态，或清掉一批僵尸连接。
     *
     * <p>返回本节点实际下线条数（并发下可能略少于调用瞬间的台账数），页面据此回显。
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
     * 推送应用列表
     *
     * <p>返回明文 apiKey 是有意为之：测试页要靠它拼鉴权头。
     * 代价是「拿到这个接口就拿到全部推送权限」——该接口必须内网隔离。
     */
    @GetMapping("/apps")
    public Map<String, Object> apps() {
        List<PushApp> list = appRegistry.list();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("total", list.size());
        result.put("authEnabled", appRegistry.isAuthEnabled());
        result.put("defaultAppId", NexusConstants.DEFAULT_APP_ID);
        // 台账位置：单机是文件路径、集群是 Redis Hash（字段名不叫 storePath，因为集群下没有文件）
        result.put("storeLocation", appRegistry.storeLocation());
        List<Map<String, Object>> items = new ArrayList<>(list.size());
        for (PushApp app : list) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("appId", app.appId());
            item.put("apiKey", app.apiKey());
            item.put("allowedModules", app.allowedModules());
            items.add(item);
        }
        result.put("items", items);
        return result;
    }

    /**
     * 生成随机密钥
     */
    @GetMapping("/apps/generate-key")
    public Map<String, Object> generateKey() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("apiKey", PushAppRegistry.generateApiKey());
        return result;
    }

    /**
     * 新增应用
     *
     * <p>内置默认应用同名一律拒绝：它始终在台账里且只读，要别的凭证（别的 appId / key / 白名单）
     * 就新建应用，而不是改它。
     *
     * <p>apiKey 留空时由服务端生成高强度随机串（UUID 128bit → Base64Url，22 字符），
     * 不自己想口令；返回值带上最终落地的 apiKey，页面回显后可直接复制。
     */
    @PostMapping("/apps")
    public Map<String, Object> addApp(@RequestBody AppRequest body) {
        if (body == null || !StringUtils.hasText(body.appId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "appId is required");
        }
        if (PushAppRegistry.isBuiltIn(body.appId())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "内置默认应用已存在且只读，请换一个 appId：" + NexusConstants.DEFAULT_APP_ID);
        }
        if (appRegistry.contains(body.appId())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "appId already exists: " + body.appId());
        }
        String appId = body.appId().trim();
        // 生成放在服务端：浏览器的随机强度不可控，且同一个 key 只应在这里产生一次
        String apiKey = StringUtils.hasText(body.apiKey())
                ? body.apiKey().trim()
                : PushAppRegistry.generateApiKey();
        List<String> modules = PushAppRegistry.normalize(body.allowedModules());
        appRegistry.save(new PushApp(appId, apiKey, modules));
        // 让其他节点立即重载：否则它们要等 app-cache-ttl 才认新凭证
        broadcastAppsChanged();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("appId", appId);
        result.put("apiKey", apiKey);
        result.put("allowedModules", modules);
        return result;
    }

    /**
     * 修改应用：字段为 null / 空串表示不修改；改动落存储，重启后仍在
     *
     * <p>内置默认应用例外：整条只读，appId / apiKey / 白名单 都改不动（有改动 → 409）；
     * 传回来的值与现状完全一致则视为「没改」，正常返回（幂等，不报错）。
     */
    @PutMapping("/apps/{appId}")
    public Map<String, Object> updateApp(@PathVariable String appId, @RequestBody AppUpdateRequest body) {
        PushApp existed = appRegistry.find(appId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "app not found: " + appId));
        if (body == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "request body is required");
        }
        // 内置默认应用只读：键和值都写死在代码里，改了等于把联调与文档的基准改掉，
        // 且所有接入方都要跟着换——要别的凭证就新建应用
        String apiKey = StringUtils.hasText(body.apiKey()) ? body.apiKey().trim() : existed.apiKey();
        List<String> modules = body.allowedModules() == null ? existed.allowedModules() : body.allowedModules();
        if (PushAppRegistry.isBuiltIn(appId)) {
            boolean keyUnchanged = NexusConstants.DEFAULT_API_KEY.equals(apiKey);
            boolean modulesUnchanged = PushAppRegistry.normalize(modules)
                    .equals(PushAppRegistry.DEFAULT_ALLOWED_MODULES);
            if (!keyUnchanged || !modulesUnchanged) {
                throw new ResponseStatusException(HttpStatus.CONFLICT,
                        "内置默认应用只读，appId / apiKey / 白名单 均不可修改：" + NexusConstants.DEFAULT_APP_ID);
            }
            // 值完全一致 → 视为没改，幂等返回（页面不会对它发编辑请求，这里防御性兜住）
            return appView(existed);
        }
        PushApp updated = new PushApp(existed.appId(), apiKey, PushAppRegistry.normalize(modules));
        appRegistry.save(updated);
        broadcastAppsChanged();
        return appView(updated);
    }

    /** 应用对象的统一出参形态：无论新增 / 修改，页面拿到的字段都齐 */
    private Map<String, Object> appView(PushApp app) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", true);
        result.put("appId", app.appId());
        result.put("apiKey", app.apiKey());
        result.put("allowedModules", app.allowedModules() == null ? List.of() : app.allowedModules());
        return result;
    }

    /**
     * 删除应用：立即失效，该 appId 之后的推送一律 401；并同步落存储，重启不会自己回来
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
     * 集群模式广播控制指令；单机没有 WsClusterBus Bean，返回 false（本地操作已完成）
     */
    private boolean publishControl(ClusterMessage message) {
        WsClusterBus bus = clusterBusProvider.getIfAvailable();
        if (bus == null) {
            return false;
        }
        try {
            bus.publishControl(message);
            return true;
        } catch (Exception e) {
            log.warn("[ws] 集群控制指令发布失败：type={}，{}", message.getType(), e.getMessage());
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
     * 只保留指定协议的条目：快照一次收集两种连接，两个接口各自分流
     */
    private static List<Map<String, Object>> filterProtocol(List<Map<String, Object>> all, String protocol) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> item : all) {
            if (protocol.equals(item.get("protocol"))) {
                result.add(item);
            }
        }
        return result;
    }

    private static int countProtocol(List<Map<String, Object>> all, String protocol) {
        int count = 0;
        for (Map<String, Object> item : all) {
            if (protocol.equals(item.get("protocol"))) {
                count++;
            }
        }
        return count;
    }

    /**
     * 按模块统计连接数（与 {@code WsClientRegistry.moduleStats()} 同口径：一条连接订阅几个模块就算几次）
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
}
