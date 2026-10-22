package com.simonking.nexus.websocket.cluster;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 快照聚合：管理页看到的是「全集群连接」，去重、坏消息、空批次三种情况必须分别算对，
 * 否则要么重复展示、要么把活着的节点误报成缺失
 *
 * @author simonking
 */
class WsClusterSnapshotCollectorTest {

    private static final List<String> BOTH_NODES = List.of("node-a", "node-b");

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void completeWhenAllExpectedNodesReplied() throws Exception {
        WsClusterSnapshotCollector.Snapshot snapshot = WsClusterSnapshotCollector.aggregate(
                List.of(batch("node-b", List.of(item("node-b", "TCP"))),
                        batch("node-a", List.of(item("node-a", "WS"), item("node-a", "WS")))),
                2, BOTH_NODES, mapper);

        assertEquals(2, snapshot.expectedNodes());
        assertEquals(2, snapshot.receivedNodes());
        assertFalse(snapshot.partial());
        assertTrue(snapshot.missingNodes().isEmpty());
        assertEquals(3, snapshot.items().size());
        // 按 nodeId 排序展平：各节点回复顺序不稳定，管理页表格不能跟着抖
        assertEquals("node-a", snapshot.items().get(0).get("nodeId"));
        assertEquals("node-b", snapshot.items().get(2).get("nodeId"));
    }

    @Test
    void duplicateReplyFromSameNodeCountsOnce() throws Exception {
        // 订阅重连可能让一个节点收到两次广播：它的连接只能算一遍
        WsClusterSnapshotCollector.Snapshot snapshot = WsClusterSnapshotCollector.aggregate(
                List.of(batch("node-a", List.of(item("node-a", "WS"))),
                        batch("node-a", List.of(item("node-a", "WS")))),
                2, BOTH_NODES, mapper);

        assertEquals(1, snapshot.receivedNodes());
        assertEquals(1, snapshot.items().size());
        assertEquals(List.of("node-b"), snapshot.missingNodes());
        assertTrue(snapshot.partial());
    }

    @Test
    void brokenBodyIsSkippedAndItsNodeStaysMissing() {
        // 单份损坏不该让整页失去数据，但缺口要被如实反映出来
        WsClusterSnapshotCollector.Snapshot snapshot = WsClusterSnapshotCollector.aggregate(
                List.of("{not-json"), 2, BOTH_NODES, mapper);

        assertEquals(0, snapshot.receivedNodes());
        assertEquals(BOTH_NODES, snapshot.missingNodes());
        assertTrue(snapshot.partial());
        assertTrue(snapshot.items().isEmpty());
    }

    @Test
    void emptyBatchCountsAsReceivedNode() throws Exception {
        // 空节点也要回一条空批次：否则发起方分不清「没连接」和「没响应」
        WsClusterSnapshotCollector.Snapshot snapshot = WsClusterSnapshotCollector.aggregate(
                List.of(batch("node-a", List.of())), 1, List.of("node-a"), mapper);

        assertEquals(1, snapshot.receivedNodes());
        assertFalse(snapshot.partial());
        assertTrue(snapshot.missingNodes().isEmpty());
        assertTrue(snapshot.items().isEmpty());
    }

    @Test
    void nullRawTreatsEveryExpectedNodeAsMissing() {
        WsClusterSnapshotCollector.Snapshot snapshot =
                WsClusterSnapshotCollector.aggregate(null, 2, BOTH_NODES, mapper);

        assertEquals(0, snapshot.receivedNodes());
        assertEquals(BOTH_NODES, snapshot.missingNodes());
        assertTrue(snapshot.partial());
    }

    @Test
    void snapshotWithoutNodeIdIsBroken() throws Exception {
        String body = mapper.writeValueAsString(
                new WsClusterSnapshotCollector.NodeSnapshot(null, 1L, List.of(item("ghost", "WS"))));

        WsClusterSnapshotCollector.Snapshot snapshot =
                WsClusterSnapshotCollector.aggregate(List.of(body), 1, List.of("node-a"), mapper);

        assertEquals(0, snapshot.receivedNodes());
        assertTrue(snapshot.missingNodes().contains("node-a"));
        assertTrue(snapshot.partial());
    }

    @Test
    void noExpectedNodeStillCountsReceivedReply() {
        // 注册表里暂时没有自己（心跳未写成功）时也至少要等一份，不能让整页永远为空
        WsClusterSnapshotCollector.Snapshot snapshot =
                WsClusterSnapshotCollector.aggregate(null, 1, List.of(), mapper);

        assertEquals(1, snapshot.expectedNodes());
        assertEquals(0, snapshot.receivedNodes());
        assertFalse(snapshot.partial(), "没有期望节点时，缺失列表为空即视为完整");
    }

    @Test
    void itemsKeepProtocolForCallerToSplit() throws Exception {
        WsClusterSnapshotCollector.Snapshot snapshot = WsClusterSnapshotCollector.aggregate(
                List.of(batch("node-a", List.of(item("node-a", "WS"), item("node-a", "TCP")))),
                1, List.of("node-a"), mapper);

        assertNotNull(snapshot.items().get(0).get("protocol"));
        assertEquals("WS", snapshot.items().get(0).get("protocol"));
        assertEquals("TCP", snapshot.items().get(1).get("protocol"));
    }

    private String batch(String nodeId, List<Map<String, Object>> items) throws Exception {
        return mapper.writeValueAsString(new WsClusterSnapshotCollector.NodeSnapshot(nodeId, 1L, items));
    }

    private static Map<String, Object> item(String nodeId, String protocol) {
        return Map.of("nodeId", nodeId, "protocol", protocol, "clientId", nodeId + "-" + protocol);
    }
}
