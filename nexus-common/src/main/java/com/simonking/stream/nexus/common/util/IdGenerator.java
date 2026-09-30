package com.simonking.stream.nexus.common.util;

/**
 * 消息ID 生成器（自研雪花布局）
 *
 * <p>布局：{@code 41 位毫秒时间戳 + 12 位节点位 + 10 位序列}，
 * 以 2024-01-01T00:00:00Z 为纪元，输出十进制字符串。
 *
 * <p>为什么单机也要带节点位：消息ID 是客户端去重、SSE {@code Last-Event-ID}
 * 的依据，集群下两个节点同一毫秒各自自增必然撞号。把节点位固定进 ID 后，
 * 单机与集群使用同一套格式，切换模式不需要改协议、也不需要迁移历史消息。
 *
 * <p>单调性：
 * <ul>
 *     <li>同一节点内严格递增（时钟回拨时沿用逻辑时间继续递增）；</li>
 *     <li>跨节点只保证唯一，不保证严格递增；客户端应结合 {@code ts} 判断新旧。</li>
 * </ul>
 *
 * <p><b>注意</b>：时间戳左移 22 位后数值约 2.4e17，超过 JavaScript
 * {@code Number.MAX_SAFE_INTEGER}（9.0e15）。消息ID 在 JSON 中始终是字符串，
 * 客户端必须按字符串处理，不要 {@code parseInt} / {@code Number()}。
 *
 * <p>生成方法用 {@code synchronized} 而不是 CAS：ID 生成不是热点路径
 * （一次推送只生成一个），锁竞争可忽略；换来的是一条时间线只由一个线程推进，
 * 不会出现「CAS 改了毫秒、序列还没重置」的竞态。
 *
 * @author simonking
 */
public final class IdGenerator {

    /**
     * 纪元：2024-01-01T00:00:00Z
     *
     * <p>用固定纪元而不是 1970：41 位时间戳从 2024 起可用约 69 年，
     * 从 1970 起只剩约 39 年（2039 年溢出）。
     */
    private static final long EPOCH = 1704067200000L;

    private static final int NODE_BITS = 12;

    private static final int SEQUENCE_BITS = 10;

    private static final long MAX_NODE = (1L << NODE_BITS) - 1;

    private static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;

    private static final int NODE_SHIFT = SEQUENCE_BITS;

    private static final int TIMESTAMP_SHIFT = NODE_BITS + SEQUENCE_BITS;

    /**
     * 节点位：0 表示单机默认；集群模式由启动时显式配置或按 nodeId 哈希下发
     */
    private static long node = 0L;

    /**
     * 上一次使用的毫秒（逻辑时间）
     */
    private static long lastMillis = EPOCH;

    /**
     * 当前毫秒内已使用的序列
     */
    private static long sequence = 0L;

    /**
     * 是否已经显式配置过节点位
     *
     * <p>同一 JVM 只允许一个节点身份：多个 Spring 上下文 / 测试上下文重复初始化时，
     * 相同节点位视为幂等；不同节点位说明同进程里出现了两个“节点”，直接失败比静默覆盖安全。
     */
    private static boolean configured = false;

    private IdGenerator() {
    }

    /**
     * 配置节点位（启动时调用一次）
     *
     * <p>集群内每个节点必须不同；相同节点位在同一毫秒内可能生成相同 ID。
     *
     * @param nodeId 0 ~ 4095
     * @throws IllegalArgumentException 越界
     */
    public static synchronized void configureNode(long nodeId) {
        if (nodeId < 0 || nodeId > MAX_NODE) {
            throw new IllegalArgumentException("节点位必须在 0~" + MAX_NODE + " 之间：" + nodeId);
        }
        if (configured) {
            if (node != nodeId) {
                throw new IllegalStateException(
                        "IdGenerator 已配置为节点位 " + node + "，拒绝重复配置为 " + nodeId
                                + "（同一 JVM 内只允许一个节点身份）");
            }
            return;
        }
        node = nodeId;
        configured = true;
    }

    /**
     * 当前节点位（诊断用）
     */
    public static synchronized long node() {
        return node;
    }

    /**
     * 生成下一个消息ID
     *
     * @return 纯数字字符串（可能超过 JS 安全整数范围，按字符串处理）
     */
    public static synchronized String nextId() {
        long now = System.currentTimeMillis();
        if (now > lastMillis) {
            lastMillis = now;
            sequence = 0L;
        } else {
            // 同一毫秒内并发，或时钟回拨：沿用逻辑时间继续递增
            sequence++;
            if (sequence > MAX_SEQUENCE) {
                // 当前毫秒 1024 个序列用尽：逻辑时间前进 1ms，等待真实时间追上
                lastMillis++;
                sequence = 0L;
            }
        }
        long value = ((lastMillis - EPOCH) << TIMESTAMP_SHIFT)
                | (node << NODE_SHIFT)
                | sequence;
        return Long.toString(value);
    }
}
