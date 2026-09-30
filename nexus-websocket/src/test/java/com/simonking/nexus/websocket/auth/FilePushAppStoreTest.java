package com.simonking.nexus.websocket.auth;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 台账路径推导回归：推错了不会报错，只会把台账静默读写到别处
 * （可执行 jar 下曾经漏到 {@code target/data}，导致「本地台账迁移」读不到源文件）
 *
 * @author simonking
 */
class FilePushAppStoreTest {

    @Test
    void ideClassesDirStaysAsIs() {
        assertEquals("E:/ws/stream-nexus/nexus-websocket/target/classes/",
                FilePushAppStore.filePathOf("file:/E:/ws/stream-nexus/nexus-websocket/target/classes/"));
    }

    @Test
    void springBoot4NestedJarStripsNestedPart() {
        // Spring Boot 4 实测写法：开头多一个斜杠，嵌套层用 /!
        assertEquals("E:/ws/stream-nexus/nexus-websocket/target/nexus-websocket-2.0.0.jar",
                FilePushAppStore.filePathOf(
                        "/nested:/E:/ws/stream-nexus/nexus-websocket/target/nexus-websocket-2.0.0.jar/!BOOT-INF/classes/!"));
    }

    @Test
    void springBoot4NestedJarOnLinuxStripsNestedPrefix() {
        // Linux 没有盘符：不剥 nested: 会把模块根推成 /nested:，台账写到根目录的错误位置
        assertEquals("/opt/app/nexus-websocket-2.0.0.jar",
                FilePushAppStore.filePathOf(
                        "/nested:/opt/app/nexus-websocket-2.0.0.jar/!BOOT-INF/classes/!"));
    }

    @Test
    void classicJarUrlStripsNestedPart() {
        assertEquals("E:/ws/stream-nexus/nexus-websocket/target/app.jar",
                FilePushAppStore.filePathOf(
                        "jar:file:/E:/ws/stream-nexus/nexus-websocket/target/app.jar!/BOOT-INF/classes!/"));
    }

    @Test
    void linuxStyleLocationKeepsLeadingSlash() {
        assertEquals("/opt/app/nexus-websocket-2.0.0.jar",
                FilePushAppStore.filePathOf(
                        "jar:file:/opt/app/nexus-websocket-2.0.0.jar!/BOOT-INF/classes!/"));
    }

    @Test
    void spacedPathIsNotMangled() {
        assertEquals("E:/My Projects/nexus-websocket/target/app.jar",
                FilePushAppStore.filePathOf(
                        "/nested:/E:/My Projects/nexus-websocket/target/app.jar/!BOOT-INF/classes/!"));
    }

    @Test
    void unescapedSpaceFallsBackToPlainPath() {
        // 空格让 URI 构造失败：必须退回 Path.of(path)，否则整个 moduleRoot 推导失效
        assertEquals("app.jar", FilePushAppStore.toPath("E:/My Projects/app.jar").getFileName().toString());
    }

    @Test
    void escapedPathIsDecoded() {
        assertEquals("app.jar", FilePushAppStore.toPath("E:/My%20Projects/app.jar").getFileName().toString());
    }

    @Test
    void relativeStorePathResolvesUnderModuleRoot() {
        // 由 target/classes 上两级得到 nexus-websocket；绝不能落到工作目录（target）下
        assertTrue(FilePushAppStore.resolve("data/push-apps.json")
                .endsWith(Path.of("nexus-websocket", "data", "push-apps.json")));
    }
}
