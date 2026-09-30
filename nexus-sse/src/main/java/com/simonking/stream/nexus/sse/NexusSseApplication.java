package com.simonking.stream.nexus.sse;

import com.simonking.stream.nexus.sse.config.AdminAuthProperties;
import com.simonking.stream.nexus.sse.config.ClusterProperties;
import com.simonking.stream.nexus.sse.config.SseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.session.data.redis.autoconfigure.SessionDataRedisAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 启动类
 *
 * <p>排除 Redis 会话的自动配置：本项目的会话共享只在集群模式启用（见
 * {@code SseSessionConfig}），依赖在 classpath 上不等于要启用——不排除的话
 * Boot 会在单机模式也建一个 Redis 会话仓库，一旦将来有人打开 Spring Session，
 * 单机就会莫名其妙要求 Redis。
 */
@EnableScheduling
@SpringBootApplication(exclude = SessionDataRedisAutoConfiguration.class)
@EnableConfigurationProperties({SseProperties.class, AdminAuthProperties.class, ClusterProperties.class})
public class NexusSseApplication {

    public static void main(String[] args) {
        SpringApplication.run(NexusSseApplication.class, args);
    }
}
