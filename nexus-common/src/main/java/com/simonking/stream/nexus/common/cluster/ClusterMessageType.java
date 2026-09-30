package com.simonking.stream.nexus.common.cluster;

/**
 * 集群消息类型
 *
 * <p>PUSH 走单独的广播频道，其余低频指令走控制频道；
 * PONG 额外走节点点对点频道，避免连接数大时把心跳应答广播给全部节点。
 *
 * @author simonking
 */
public enum ClusterMessageType {

    /**
     * 业务推送：携带完整消息 JSON，所有节点收到后按模块 / clientId 本地扇出
     */
    PUSH,

    /**
     * 心跳应答转发：把 PONG 从「收到请求的节点」转给「持有连接的节点」
     */
    PONG,

    /**
     * 强制下线单条连接：所有节点检查本地是否存在该 clientId
     */
    KICK,

    /**
     * 强制下线全部连接：所有节点清空本地注册表
     */
    KICK_ALL,

    /**
     * 应用台账已变更：各节点重新加载本地缓存
     */
    APPS_CHANGED,

    /**
     * 管理台账快照请求：各节点把本节点连接视图 LPUSH 到 replyKey
     */
    SNAPSHOT_REQUEST
}
