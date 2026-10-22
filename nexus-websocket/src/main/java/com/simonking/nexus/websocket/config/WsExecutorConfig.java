package com.simonking.nexus.websocket.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * WebSocket 集群相关线程池
 *
 * <p>两类任务分开，互不抢占：
 * <ul>
 *   <li>{@code wsFanoutExecutor}：推送本地扇出，一条消息可能写上万条连接；</li>
 *   <li>{@code wsRedisListenerExecutor}：Redis 消息分发，默认的 {@code SimpleAsyncTaskExecutor}
 *       每条消息新建一个线程，高频推送下线程创建/销毁本身就是瓶颈。</li>
 * </ul>
 *
 * <p>不共用 {@code ForkJoinPool.commonPool}（会被 JVM 内其它并行流拖慢），
 * 也不共用 Netty 的 EventLoop（写入虽快，但排队会拖慢同一条链路上的其它连接）。
 *
 * <p>不需要「连接归属写入」线程池：WebSocket 的 PONG 走同一条 TCP 连接、必然回到
 * 持有节点，集群模式不存在「心跳应答落到别的节点」这种情况，也就没有 owners 索引。
 *
 * <p>拒绝策略统一用 {@link ThreadPoolExecutor.CallerRunsPolicy}：队列满时回退到调用线程，
 * 宁可这一轮慢一点，也不静默丢消息。
 *
 * @author simonking
 */
@Configuration(proxyBeanMethods = false)
public class WsExecutorConfig {

    @Bean(name = "wsFanoutExecutor")
    public ThreadPoolTaskExecutor wsFanoutExecutor() {
        return executor("ws-fanout-", 0, 0, 2000);
    }

    /**
     * Redis 消息分发：集群模式下每个节点都会收到全量广播，单条处理要够快
     */
    @Bean(name = "wsRedisListenerExecutor")
    public ThreadPoolTaskExecutor wsRedisListenerExecutor() {
        return executor("ws-redis-listener-", 2, 4, 10000);
    }

    /**
     * 订阅握手单独一个池：订阅阻塞时不能把消息分发一起卡住
     */
    @Bean(name = "wsRedisSubscriptionExecutor")
    public ThreadPoolTaskExecutor wsRedisSubscriptionExecutor() {
        return executor("ws-redis-subscription-", 1, 2, 1000);
    }

    private static ThreadPoolTaskExecutor executor(String prefix, int core, int max, int queueCapacity) {
        int cpus = Runtime.getRuntime().availableProcessors();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(core > 0 ? core : Math.max(2, cpus));
        executor.setMaxPoolSize(max > 0 ? max : Math.max(4, cpus * 2));
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(prefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(5);
        return executor;
    }
}
