package com.simonking.stream.nexus.sse.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 会话命名空间必须跟着总线前缀走：SSE 与 WebSocket 共用一个 Redis 时，会话 Key 不能撞
 *
 * @author simonking
 */
class SseSessionConfigTest {

    @Test
    void namespaceFollowsKeyPrefix() {
        ClusterProperties properties = new ClusterProperties();
        assertEquals("nexus:sse:session", SseSessionConfig.sessionNamespace(properties));

        properties.setKeyPrefix("nexus:ws");
        assertEquals("nexus:ws:session", SseSessionConfig.sessionNamespace(properties));
    }
}
