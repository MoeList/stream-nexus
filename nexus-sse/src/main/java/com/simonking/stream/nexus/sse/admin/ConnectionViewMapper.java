package com.simonking.stream.nexus.sse.admin;

import com.simonking.stream.nexus.common.location.IpLocationService;
import com.simonking.stream.nexus.sse.config.SseProperties;
import com.simonking.stream.nexus.sse.connection.SseClient;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 连接视图转换
 *
 * <p>管理页与集群快照必须用同一套字段口径：管理页聚合多节点快照时，
 * 远端节点已经把 {@code city} 算好，本地直接透传；如果两边各写一份 toView，
 * 迟早会出现「单机页有 city、集群页没有」这类只在某种部署下复现的差异。
 *
 * @author simonking
 */
@Component
@RequiredArgsConstructor
public class ConnectionViewMapper {

    private final SseProperties properties;

    private final IpLocationService ipLocation;

    /**
     * 单条连接的展示视图
     */
    public Map<String, Object> toView(SseClient client, long now) {
        // 时钟回拨 / 刚建连时可能算出负数，归零
        long silence = Math.max(now - client.getLastPongTime(), 0);
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("clientId", client.getClientId());
        view.put("ip", client.getIp() == null ? "-" : client.getIp());
        // 归属地：缺库时下发 "-"，页面据此不展示这一行，避免出现空的城市位
        view.put("city", ipLocation.city(client.getIp()));
        view.put("modules", client.getModules());
        view.put("createTime", client.getCreateTime());
        view.put("lastPongTime", client.getLastPongTime());
        view.put("online", Math.max(now - client.getCreateTime(), 0));
        view.put("silence", silence);
        view.put("stale", silence > properties.getHeartbeatTimeout().toMillis());
        return view;
    }

    /**
     * 集群快照里的单条连接：额外带上所在节点
     *
     * <p>聚合后所有节点的连接混在一张表里，没有 {@code nodeId} 就分不清
     * 「这条连接在哪踢」——强制下线会发到全集群，但排障时仍要知道源头。
     */
    public Map<String, Object> toClusterView(SseClient client, long now, String nodeId) {
        Map<String, Object> view = toView(client, now);
        view.put("nodeId", nodeId);
        return view;
    }
}
