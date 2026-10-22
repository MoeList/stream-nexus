package com.simonking.stream.nexus.sse.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import com.simonking.stream.nexus.sse.core.AbstractSseBroadcaster;
import com.simonking.stream.nexus.sse.core.SseSender;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.Executor;

/**
 * 集群广播器：先发布到 Redis，再在入口节点本地扇出
 *
 * <p>顺序不能反：先发布再本地发送，保证「入口节点发出去了」时其他节点至少已经收到广播。
 * 如果先本地发、再发布失败，调用方会看到部分成功，但其他节点永远收不到，无法补偿。
 *
 * <p>消费端通过 {@link SseClusterDispatcher} 调用 {@link #pushLocal(ClusterMessage)}，
 * 这里会跳过 {@code originNodeId == 本节点} 的消息，避免入口节点重复发送。
 *
 * <p><b>统计口径</b>：返回的 {@code total/success/failed} 只是入口节点本节点的扇出结果，
 * 不是全集群送达数；{@code clusterNodes} 是发布瞬间的在线节点数（含入口节点），
 * 取自节点注册表的本地快照，不额外增加 Redis 往返。
 *
 * @author simonking
 */
public class RedisSseBroadcaster extends AbstractSseBroadcaster {

    private final ClusterBus clusterBus;

    private final ClusterNodeIdentity identity;

    private final ClusterNodeRegistry nodeRegistry;

    public RedisSseBroadcaster(SseClientRegistry registry, SseSender sender, ObjectMapper objectMapper,
                               ClusterBus clusterBus, ClusterNodeIdentity identity,
                               ClusterNodeRegistry nodeRegistry, Executor fanoutExecutor) {
        super(registry, sender, objectMapper, fanoutExecutor);
        this.clusterBus = clusterBus;
        this.identity = identity;
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    public PushResult push(PushRequest request) {
        ClusterMessage message = buildMessage(request);
        try {
            clusterBus.publishPush(message);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, e.getMessage(), e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "cluster publish failed", e);
        }

        // 发布成功后再本地扇出；入口节点不能走 pushLocal（会被 origin 判断跳过）
        PushResult local = super.pushLocal(message);
        return PushResult.builder()
                .messageId(local.getMessageId())
                .total(local.getTotal())
                .success(local.getSuccess())
                .failed(local.getFailed())
                .mode(PushResult.MODE_CLUSTER)
                .published(true)
                .originNodeId(identity.getNodeId())
                .clusterNodes(nodeRegistry.onlineCount())
                .scope(PushResult.SCOPE_NODE)
                .build();
    }

    @Override
    public PushResult pushLocal(ClusterMessage message) {
        if (identity.getNodeId().equals(message.getOriginNodeId())) {
            return PushResult.builder()
                    .messageId(message.getMessageId())
                    .total(0)
                    .success(0)
                    .failed(0)
                    .build();
        }
        return super.pushLocal(message);
    }

    @Override
    protected String originNodeId() {
        return identity.getNodeId();
    }
}
