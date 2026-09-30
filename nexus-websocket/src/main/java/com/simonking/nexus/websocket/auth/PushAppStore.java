package com.simonking.nexus.websocket.auth;

import java.util.List;

/**
 * 推送应用台账的持久化端口
 *
 * <p>两种实现，按 {@code nexus.ws.cluster.enabled} 二选一：
 * <ul>
 *     <li>{@link FilePushAppStore}：单机，本地 JSON 文件；</li>
 *     <li>{@link RedisPushAppStore}：集群，Redis Hash + 版本号，多节点共享同一份台账。</li>
 * </ul>
 *
 * <p><b>为什么是单条写入而不是全量覆盖</b>：多节点同时改台账时，全量覆盖会让
 * 「后写者把先写者的新增冲掉」。按 appId 单条增删，各节点的修改天然互不干扰，
 * 也让 Redis 实现可以是一条 HSET / HDEL。
 *
 * <p><b>写入顺序</b>：先写数据、再递增版本号。反过来会让其他节点先看到「版本变了」
 * 却读到旧数据，而且在下一次版本变化之前一直是旧的。
 *
 * @author simonking
 */
public interface PushAppStore {

    /**
     * 台账是否已初始化
     *
     * <p>与「读取结果为空」区分开：用户把应用删光是合法状态，
     * 此时**不能**重新播种内置默认应用，否则「删光后重启又被塞回一个 test-demo」很反直觉。
     * {@link RedisPushAppStore} 用版本号 Key 是否存在来表示「写过一次」。
     */
    boolean exists();

    /**
     * 读取全量台账
     */
    List<PushApp> load();

    /**
     * 按 appId 新增或覆盖单条
     *
     * @throws IllegalStateException 持久化失败：内存已改而存储没落，属于必须让人知道的错误
     */
    void save(PushApp app);

    /**
     * 按 appId 删除单条
     *
     * @return 存储里是否真的删掉了一条（内存侧的判断由调用方负责）
     */
    boolean delete(String appId);

    /**
     * 台账版本号：每次写操作递增，供各节点判断本地缓存是否过期
     *
     * <p>不支持版本的文件实现固定返回 {@code -1}，调用方的「版本没变就不重载」逻辑
     * 因此永远成立——单机本来就没有第二个写者。
     */
    long version();

    /**
     * 台账位置的可读描述（管理页展示用，不做 IO）
     */
    String describe();
}
