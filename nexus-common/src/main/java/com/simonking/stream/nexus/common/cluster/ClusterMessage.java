package com.simonking.stream.nexus.common.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 集群总线消息信封
 *
 * <p>跨节点传输统一走这一个结构，避免为每种指令各定义一套 JSON：
 * 频率低、体积小，多几个空字段比多几个类更容易演进。
 *
 * <p>推送消息（{@link ClusterMessageType#PUSH}）的 {@link #message} 字段是
 * <b>已经序列化好的完整 {@code NexusMessage} JSON 字符串</b>：
 * 消费节点直接把它写进 SSE，不再反序列化 {@code data} 再序列化——
 * 既保留原始类型（数字不会变成 Integer/Double），也省一次对象转换。
 *
 * <p>{@code v} 是协议版本，消费端不认识时丢弃并告警；未知字段反序列化时忽略，
 * 便于先升级消费端、再升级生产端。
 *
 * @author simonking
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClusterMessage {

    /**
     * 协议版本，见 {@link ClusterConstants#PROTOCOL_VERSION}
     */
    @Builder.Default
    private int v = ClusterConstants.PROTOCOL_VERSION;

    /**
     * 消息类型
     */
    private ClusterMessageType type;

    /**
     * 生产这条消息的节点ID：PUSH 回环到生产节点时据此跳过，避免重复发送
     */
    private String originNodeId;

    /**
     * 消息ID：PUSH 时由入口节点生成，所有节点共用同一个值
     */
    private String messageId;

    /**
     * 目标业务模块：PUSH 的路由依据之一
     */
    private String bizModule;

    /**
     * 定向推送的客户端ID列表：PUSH 的路由依据之二
     */
    private List<String> clientIds;

    /**
     * SSE 事件名（MESSAGE / PING 等）
     */
    private String event;

    /**
     * 业务动作
     */
    private String action;

    /**
     * 服务端时间戳（毫秒）
     */
    private Long ts;

    /**
     * 完整 {@code NexusMessage} JSON 字符串（仅 PUSH）
     */
    private String message;

    /**
     * 目标客户端ID（仅 PONG / KICK）
     */
    private String clientId;

    /**
     * 快照请求ID（仅 SNAPSHOT_REQUEST）
     */
    private String requestId;

    /**
     * 快照响应写入的 Redis List key（仅 SNAPSHOT_REQUEST）
     */
    private String replyKey;
}
