package com.simonking.nexus.websocket.core;

import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;

/**
 * WebSocket 广播策略
 *
 * <p>单机与集群的差异只在这里：单机直接本地扇出；集群先发布到 Redis，再本地扇出。
 * 上层（{@link WsPusher}、{@link PushService}、{@link TcpPushService}）不感知运行模式。
 *
 * <p>实现由 {@code WsBroadcasterConfig} 用 {@code @ConditionalOnProperty} 二选一注册，
 * 同一时刻容器里只应存在一个实现——注入点一律用本接口，不要按实现类注入。
 *
 * <p><b>与 SSE 的差异</b>：WebSocket 有两个推送入口（REST {@code /ws/push} 与 TCP 9091），
 * 两者共用本接口，所以「本地没有目标连接」绝不能作为跳过跨节点广播的理由——
 * 业务系统可能把消息交给 A 节点，而目标终端连在 B 节点上。
 *
 * <p>不需要 PONG 转发：WebSocket 的心跳应答走同一条 TCP 连接，必然回到持有连接的节点，
 * 因此没有 SSE 那套 owners 索引与点对点频道。
 *
 * @author simonking
 */
public interface WsBroadcaster {

    /**
     * 推送入口：生成消息ID，按当前模式广播
     *
     * @return 入口节点统计结果
     */
    PushResult push(PushRequest request);

    /**
     * 只在本节点扇出（集群消费端 / 单机本地路径共用）
     *
     * <p>集群实现会跳过 {@code originNodeId == 本节点} 的消息，避免入口节点重复发送；
     * 单机实现直接扇出。
     */
    PushResult pushLocal(ClusterMessage message);
}
