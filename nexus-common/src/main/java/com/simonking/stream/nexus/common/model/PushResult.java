package com.simonking.stream.nexus.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 推送结果
 *
 * <p>单机模式下只有前四个字段；集群模式追加运行模式与发布状态。
 *
 * <p><b>统计口径</b>：{@link #total} / {@link #success} / {@link #failed}
 * 始终是<b>入口节点本节点</b>的扇出结果，不是全集群送达数。
 * 集群模式下消息由入口节点发布到 Redis 后，其他节点异步消费并扇出，
 * 入口节点不会等待它们的回执（等待会让一次推送被最慢节点拖住）。
 * 需要全集群精确统计时，应通过管理台账或另行实现回执聚合。
 *
 * <p>两种模式共有的字段：前四个 + {@link #mode} + {@link #scope}；
 * {@link #published} / {@link #originNodeId} / {@link #clusterNodes} 只存在于集群模式，
 * 其余情况序列化时直接省略（{@code NON_NULL}）。
 *
 * @author simonking
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PushResult {

    /**
     * 运行模式：单机（未开启集群）
     */
    public static final String MODE_STANDALONE = "standalone";

    /**
     * 运行模式：集群（已开启跨节点广播）
     */
    public static final String MODE_CLUSTER = "cluster";

    /**
     * 统计范围：始终是入口节点本节点，不是全集群送达数
     */
    public static final String SCOPE_NODE = "node";

    /**
     * 本次生成的消息ID
     */
    private String messageId;

    /**
     * 命中的连接数（入口节点）
     */
    private int total;

    /**
     * 发送成功的连接数（入口节点）
     */
    private int success;

    /**
     * 发送失败（已回收）的连接数（入口节点）
     */
    private int failed;

    /**
     * 运行模式：{@link #MODE_STANDALONE} 单机 / {@link #MODE_CLUSTER} 集群
     */
    private String mode;

    /**
     * 集群模式下是否已成功发布到 Redis；发布失败会直接返回 503，不会走到这里。
     * 单机模式为 null（不参与序列化）
     */
    private Boolean published;

    /**
     * 集群模式下处理本次请求的入口节点ID；单机为 null
     */
    private String originNodeId;

    /**
     * 发布时在线的节点数（含入口节点），仅集群模式
     */
    private Integer clusterNodes;

    /**
     * 统计范围：固定 {@link #SCOPE_NODE}，提醒调用方 total 只代表入口节点
     */
    private String scope;
}
