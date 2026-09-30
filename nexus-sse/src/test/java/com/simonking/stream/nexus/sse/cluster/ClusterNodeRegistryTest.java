package com.simonking.stream.nexus.sse.cluster;

import com.simonking.stream.nexus.common.cluster.ClusterNodeInfo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 启动唯一性校验：直接决定「这个进程能不能起来」，必须覆盖重启与双开两种边界
 *
 * @author simonking
 */
class ClusterNodeRegistryTest {

    private static final long TIMEOUT = 30_000L;

    private static final long NOW = 1_700_000_000_000L;

    @Test
    void noConflictWhenNoOtherNodes() {
        assertNull(ClusterNodeRegistry.detectConflict(self(), List.of(), NOW, TIMEOUT));
    }

    @Test
    void nodeSeqConflictWithAliveNode() {
        String conflict = ClusterNodeRegistry.detectConflict(
                self(), List.of(node("node-2", 7, "inst-2", NOW - 1_000L)), NOW, TIMEOUT);
        assertNotNull(conflict);
        assertTrue(conflict.contains("节点位冲突"), conflict);
        assertTrue(conflict.contains("node-2"), conflict);
    }

    @Test
    void nodeSeqReusableAfterOwnerExpired() {
        // 超过 node-timeout 未续期：它的节点位可以被新节点接管
        assertNull(ClusterNodeRegistry.detectConflict(
                self(), List.of(node("node-2", 7, "inst-2", NOW - TIMEOUT - 1)), NOW, TIMEOUT));
    }

    @Test
    void sameNodeIdStillHeartbeatingIsDoubleStart() {
        // 旧实例在本进程启动之后仍写过心跳 → 两个进程同时在跑
        String conflict = ClusterNodeRegistry.detectConflict(
                self(), List.of(node("node-1", 7, "inst-old", NOW)), NOW, TIMEOUT);
        assertNotNull(conflict);
        assertTrue(conflict.contains("双开"), conflict);
    }

    @Test
    void sameNodeIdFromPreviousRunIsRestart() {
        // 旧实例的最后心跳早于本进程启动 → 只是记录没过期，属于重启
        assertNull(ClusterNodeRegistry.detectConflict(
                self(), List.of(node("node-1", 7, "inst-old", NOW - 6_000L)), NOW, TIMEOUT));
    }

    @Test
    void sameInstanceIsNeverConflict() {
        assertNull(ClusterNodeRegistry.detectConflict(
                self(), List.of(node("node-1", 7, "inst-1", NOW)), NOW, TIMEOUT));
    }

    @Test
    void brokenRecordWithoutNodeIdIsIgnored() {
        ClusterNodeInfo broken = ClusterNodeInfo.builder().lastSeen(NOW).build();
        assertNull(ClusterNodeRegistry.detectConflict(self(), List.of(broken), NOW, TIMEOUT));
    }

    private static ClusterNodeInfo self() {
        return ClusterNodeInfo.builder()
                .nodeId("node-1")
                .nodeSeq(7)
                .instanceId("inst-1")
                .startTime(NOW - 5_000L)
                .lastSeen(NOW)
                .build();
    }

    private static ClusterNodeInfo node(String nodeId, int nodeSeq, String instanceId, long lastSeen) {
        return ClusterNodeInfo.builder()
                .nodeId(nodeId)
                .nodeSeq(nodeSeq)
                .instanceId(instanceId)
                .startTime(lastSeen)
                .lastSeen(lastSeen)
                .build();
    }
}
