package com.simonking.nexus.websocket.admin;

import com.simonking.nexus.websocket.config.WsProperties;
import com.simonking.nexus.websocket.model.TcpClient;
import com.simonking.nexus.websocket.model.WsClient;
import com.simonking.stream.nexus.common.location.IpLocationService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 连接视图转换
 *
 * <p>管理页与集群快照必须用同一套字段口径：管理页聚合多节点快照时，
 * 远端节点已经把 {@code city} / {@code protocol} 算好，本地直接透传；如果两边各写一份 toView，
 * 迟早会出现「单机页有 city、集群页没有」这类只在某种部署下复现的差异。
 *
 * <p>WebSocket 与 TCP 两种连接都在这里转换：管理页是两张表，但集群快照<b>一次收集两种</b>
 * （每批里各条目带 {@code protocol}，调用方按它分流），否则同一份 Redis 往返要跑两遍。
 *
 * @author simonking
 */
@Component
@RequiredArgsConstructor
public class ConnectionViewMapper {

    /**
     * 协议标识：页面据此区分一条连接是终端的 WebSocket 长连接，
     * 还是业务系统侧的 TCP 长连接
     */
    public static final String PROTOCOL_WS = "WS";

    public static final String PROTOCOL_TCP = "TCP";

    private final WsProperties properties;

    private final IpLocationService ipLocation;

    /**
     * 单条 WebSocket 连接的展示视图
     */
    public Map<String, Object> toView(WsClient client, long now) {
        // 时钟回拨 / 刚建连时可能算出负数，归零
        long silence = Math.max(now - client.getLastPongTime(), 0);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("protocol", PROTOCOL_WS);
        view.put("clientId", client.getClientId());
        view.put("ip", client.getIp() == null ? "-" : client.getIp());
        // 归属地：缺库时下发 "-"，页面据此不展示这一行，避免出现空的城市位
        view.put("city", ipLocation.city(client.getIp()));
        view.put("modules", client.getModules());
        view.put("createTime", client.getCreateTime());
        view.put("lastPongTime", client.getLastPongTime());
        view.put("lastActiveTime", client.getLastActiveTime());
        view.put("online", Math.max(now - client.getCreateTime(), 0));
        view.put("silence", silence);
        view.put("stale", silence > properties.getHeartbeatTimeout().toMillis());
        view.put("uplinkCount", client.getUplinkCount().get());
        view.put("downlinkCount", client.getDownlinkCount().get());
        return view;
    }

    /**
     * 单条 TCP 接入连接的展示视图
     */
    public Map<String, Object> toTcpView(TcpClient client, long now) {
        long silence = Math.max(now - client.getLastPongTime(), 0);
        Map<String, Object> view = new LinkedHashMap<>();
        // 协议标识：TCP 台账与终端台账字段不同（通道ID / 报文数 vs 客户端ID / 订阅模块），
        // 前端合并展示或筛选时靠它区分
        view.put("protocol", PROTOCOL_TCP);
        view.put("channelId", client.getChannelId());
        view.put("ip", client.getIp() == null ? "-" : client.getIp());
        view.put("createTime", client.getCreateTime());
        view.put("lastActiveTime", client.getLastActiveTime());
        view.put("lastPongTime", client.getLastPongTime());
        view.put("online", Math.max(now - client.getCreateTime(), 0));
        view.put("silence", silence);
        view.put("stale", silence > properties.getHeartbeatTimeout().toMillis());
        view.put("msgCount", client.getMsgCount().get());
        return view;
    }

    /**
     * 集群快照里的 WebSocket 连接：额外带上所在节点
     *
     * <p>聚合后所有节点的连接混在一张表里，没有 {@code nodeId} 就分不清
     * 「这条连接在哪踢」——强制下线会发到全集群，但排障时仍要知道源头。
     */
    public Map<String, Object> toClusterView(WsClient client, long now, String nodeId) {
        return withNode(toView(client, now), nodeId);
    }

    /**
     * 集群快照里的 TCP 连接：同理带上节点
     */
    public Map<String, Object> toTcpClusterView(TcpClient client, long now, String nodeId) {
        return withNode(toTcpView(client, now), nodeId);
    }

    private static Map<String, Object> withNode(Map<String, Object> view, String nodeId) {
        view.put("nodeId", nodeId);
        return view;
    }
}
