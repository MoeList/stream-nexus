package com.simonking.nexus.websocket;

import com.simonking.nexus.websocket.config.AdminAuthProperties;
import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.nexus.websocket.config.WsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.session.data.redis.autoconfigure.SessionDataRedisAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 独立部署的 WebSocket 推送服务
 *
 * <p>一个进程三组端口：
 * <ul>
 *     <li>{@code 8089}：HTTP，REST 推送接口（{@code POST /ws/push}）+ 管理界面 + 推送测试页 + 运维接口；</li>
 *     <li>{@code 9090}：Netty WebSocket，终端建长连接，只负责收推送；</li>
 *     <li>{@code 9091}：Netty TCP，业务系统建长连接，把待推消息交进来后由本服务扇出到终端。</li>
 * </ul>
 *
 * <p>后两者共享同一张终端连接注册表，因此 TCP 收到消息后可以直接写终端的 Channel，
 * 不需要任何跨进程转发——这是「WebSocket 服务独立部署、业务系统只引客户端 SDK」能成立的前提。
 *
 * <p>排除 Redis 会话的自动配置：本项目的会话共享只在集群模式启用（见
 * {@code WsSessionConfig}），依赖在 classpath 上不等于要启用——不排除的话 Boot 会在
 * 单机模式也建一个 Redis 会话仓库，单机就莫名其妙要求有 Redis。
 *
 * <p>开启定时任务：节点心跳（{@code WsClusterNodeRegistry} 的 {@code @Scheduled}）依赖它，
 * 单机模式没有该 Bean，定时任务框架空转不影响任何行为。
 *
 * @author simonking
 */
@EnableScheduling
@SpringBootApplication(exclude = SessionDataRedisAutoConfiguration.class)
@EnableConfigurationProperties({WsProperties.class, AdminAuthProperties.class, WsClusterProperties.class})
public class NexusWebsocketApplication {

    public static void main(String[] args) {
        SpringApplication.run(NexusWebsocketApplication.class, args);
    }

}
