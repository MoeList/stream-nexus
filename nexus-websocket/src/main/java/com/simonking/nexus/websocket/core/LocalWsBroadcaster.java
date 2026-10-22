package com.simonking.nexus.websocket.core;

import com.simonking.nexus.websocket.registry.WsClientRegistry;
import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.Executor;

/**
 * 单机广播器：生成消息后直接在本节点扇出
 *
 * <p>只有当 {@code nexus.ws.cluster.enabled=false}（含未配置）时才会被注册。
 *
 * <p>与改造前的 {@code WsPusher} 行为一致，唯一区别是：本地没有目标连接时不再提前返回，
 * 而是照常构建消息、走统一的扇出路径（结果为 total=0）。这个短路在集群模式下会造成
 * 跨节点漏推，两个实现共用同一条路径才能保证「单机推得到、集群也推得到」。
 *
 * @author simonking
 */
public class LocalWsBroadcaster extends AbstractWsBroadcaster {

    public LocalWsBroadcaster(WsClientRegistry registry, ObjectMapper objectMapper, Executor fanoutExecutor) {
        super(registry, objectMapper, fanoutExecutor);
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
