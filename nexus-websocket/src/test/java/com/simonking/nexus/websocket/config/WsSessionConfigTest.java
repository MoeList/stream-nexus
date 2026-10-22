package com.simonking.nexus.websocket.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 会话命名空间必须跟着总线前缀走：SSE 与 WebSocket 共用一个 Redis 时，会话 Key 不能撞
 *
 * @author simonking
 */
class WsSessionConfigTest {

    @Test
    void namespaceFollowsKeyPrefix() {
        WsClusterProperties properties = new WsClusterProperties();
        assertEquals("nexus:ws:session", WsSessionConfig.sessionNamespace(properties));

        properties.setKeyPrefix("nexus:ws:preview");
        assertEquals("nexus:ws:preview:session", WsSessionConfig.sessionNamespace(properties));
    }
}
