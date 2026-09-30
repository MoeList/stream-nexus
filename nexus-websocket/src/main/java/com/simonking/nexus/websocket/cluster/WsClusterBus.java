package com.simonking.nexus.websocket.cluster;

import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * 集群总线发布端
 *
 * <p>只负责「把信封序列化后发到哪个频道」，不持有监听容器、不处理消息：
 * 发布与订阅分开，避免和消费分发形成循环依赖。
 *
 * <p>频道划分（WebSocket 比 SSE 少一个）：
 * <ul>
 *     <li>{@code bus:push}：业务推送，所有节点订阅；</li>
 *     <li>{@code bus:control}：KICK / KICK_ALL / APPS_CHANGED / SNAPSHOT_REQUEST。</li>
 * </ul>
 * 没有 {@code node:{nodeId}} 点对点频道：那是给 SSE 的 PONG 定向转发用的，
 * 而 WebSocket 的心跳应答走同一条 TCP 连接，必然回到持有连接的节点。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "true")
public class WsClusterBus {

    private final StringRedisTemplate redis;

    private final WsClusterProperties properties;

    private final ObjectMapper objectMapper;

    /**
     * 发布业务推送：所有节点都会收到，消费端按 {@code originNodeId} 跳过回环
     */
    public void publishPush(ClusterMessage message) {
        publish(ClusterConstants.channelPush(properties.getKeyPrefix()), message, true);
    }

    /**
     * 发布控制指令：KICK / KICK_ALL / APPS_CHANGED / SNAPSHOT_REQUEST
     */
    public void publishControl(ClusterMessage message) {
        publish(ClusterConstants.channelControl(properties.getKeyPrefix()), message, false);
    }

    private void publish(String channel, ClusterMessage message, boolean checkSize) {
        String json = objectMapper.writeValueAsString(message);
        if (checkSize) {
            int bytes = json.getBytes(StandardCharsets.UTF_8).length;
            if (bytes > properties.getMaxMessageBytes()) {
                throw new IllegalArgumentException(
                        "推送消息体过大：" + bytes + " 字节，超过上限 " + properties.getMaxMessageBytes());
            }
        }
        redis.convertAndSend(channel, json);
    }
}
