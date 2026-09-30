package com.simonking.nexus.websocket.cluster;

import com.simonking.nexus.websocket.core.AbstractWsBroadcaster;
import com.simonking.nexus.websocket.registry.WsClientRegistry;
import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
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
 * <p><b>绝不因为本地没有目标就跳过发布</b>：这是 WebSocket 集群化最容易踩的坑。
 * 业务系统可能把消息交给 A 节点，而所有目标终端都连在 B 节点上——A 节点本地
 * {@code resolveTargets} 返回空集合，但消息必须照样广播出去。
 *
 * <p>消费端通过 {@link WsClusterDispatcher} 调用 {@link #pushLocal(ClusterMessage)}，
 * 这里会跳过 {@code originNodeId == 本节点} 的消息，避免入口节点重复发送。
 *
 * <p><b>统计口径</b>：返回的 {@code total/success/failed} 只是入口节点本节点的扇出结果，
 * 不是全集群送达数；{@code clusterNodes} 是发布瞬间的在线节点数（含入口节点），
 * 取自节点注册表的本地快照，不额外增加 Redis 往返。
 *
 * <p><b>异常语义</b>：发布失败区分两种——消息体超限抛 413（调用方的数据问题），
 * 其余 Redis 故障抛 503（服务端问题）。REST 入口直接映射为状态码；
 * TCP 入口由 {@code TcpFrameHandler} 捕获 {@link ResponseStatusException} 后
 * 把原因写进 ERROR 报文，而不是冒泡断连接。
 *
 * @author simonking
 */
public class RedisWsBroadcaster extends AbstractWsBroadcaster {

    private final WsClusterBus clusterBus;

    private final WsClusterNodeIdentity identity;

    private final WsClusterNodeRegistry nodeRegistry;

    public RedisWsBroadcaster(WsClientRegistry registry, ObjectMapper objectMapper,
                              WsClusterBus clusterBus, WsClusterNodeIdentity identity,
                              WsClusterNodeRegistry nodeRegistry, Executor fanoutExecutor) {
        super(registry, objectMapper, fanoutExecutor);
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
