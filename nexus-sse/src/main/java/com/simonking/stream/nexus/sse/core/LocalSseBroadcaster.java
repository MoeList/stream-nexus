package com.simonking.stream.nexus.sse.core;

import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.Executor;

/**
 * 单机广播器：生成消息后直接在本节点扇出
 *
 * <p>只有当 {@code nexus.sse.cluster.enabled=false}（含未配置）时才会被注册。
 *
 * @author simonking
 */
public class LocalSseBroadcaster extends AbstractSseBroadcaster {

    public LocalSseBroadcaster(SseClientRegistry registry, SseSender sender,
                               ObjectMapper objectMapper, Executor fanoutExecutor) {
        super(registry, sender, objectMapper, fanoutExecutor);
    }

    @Override
    public PushResult push(PushRequest request) {
        PushResult local = pushLocal(buildMessage(request));
        // mode / scope 两种模式都填：调用方不必先判空才知道自己跑在哪种形态下，
        // published / originNodeId / clusterNodes 才是集群专属（NON_NULL 序列化时省略）
        return PushResult.builder()
                .messageId(local.getMessageId())
                .total(local.getTotal())
                .success(local.getSuccess())
                .failed(local.getFailed())
                .mode(PushResult.MODE_STANDALONE)
                .scope(PushResult.SCOPE_NODE)
                .build();
    }

    @Override
    protected String originNodeId() {
        // 单机没有节点身份，也不需要回环判断
        return null;
    }
}
