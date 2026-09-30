package com.simonking.nexus.websocket.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterConstants;
import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * 集群总线订阅端
 *
 * <p>整个进程只注册一个实例、只挂到一个 {@code RedisMessageListenerContainer} 上；
 * 收到消息后按信封类型交给 {@link WsClusterDispatcher}。不要为每个频道 new 一个
 * listener——那会让同一条广播被本节点消费多次。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "nexus.ws.cluster.enabled", havingValue = "true")
public class WsClusterMessageListener implements MessageListener {

    private final ObjectMapper objectMapper;

    private final WsClusterDispatcher dispatcher;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            ClusterMessage clusterMessage = objectMapper.readValue(body, ClusterMessage.class);
            if (clusterMessage == null || clusterMessage.getType() == null) {
                log.warn("[ws] 忽略缺少类型的集群消息");
                return;
            }
            if (clusterMessage.getV() != ClusterConstants.PROTOCOL_VERSION) {
                log.warn("[ws] 忽略协议版本不匹配的集群消息：v={}, type={}",
                        clusterMessage.getV(), clusterMessage.getType());
                return;
            }
            dispatcher.dispatch(clusterMessage);
        } catch (Exception e) {
            log.warn("[ws] 集群消息处理失败：{}", e.getMessage());
        }
    }
}
