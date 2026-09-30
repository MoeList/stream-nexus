package com.simonking.stream.nexus.sse.core;

import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;

/**
 * SSE 广播策略
 *
 * <p>单机与集群的差异只在这里：单机直接本地扇出；集群先发布到 Redis，再本地扇出。
 * 上层的 {@link SsePusher} 与 {@code PushController} 不感知运行模式。
 *
 * <p>实现由 {@code SseBroadcasterConfig} 用 {@code @ConditionalOnProperty} 二选一注册，
 * 同一时刻容器里只应存在一个实现——注入点一律用本接口，不要按实现类注入。
 *
 * @author simonking
 */
public interface SseBroadcaster {

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
