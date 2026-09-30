package com.simonking.stream.nexus.sse.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * SSE 专用线程池
 *
 * <p>三类任务分开，互不抢占：
 * <ul>
 *   <li>{@code sseFanoutExecutor}：推送本地扇出，一条消息可能写上万条连接；</li>
 *   <li>{@code sseHeartbeatExecutor}：心跳下发，定时批量、单轮必须能跑完；</li>
 *   <li>{@code sseOwnerExecutor}：集群连接归属写入，Redis 抖动时不能阻塞建连线程。</li>
 * </ul>
 *
 * <p>不共用 {@code ForkJoinPool.commonPool}（会被 JVM 内其它并行流拖慢），
 * 也不共用 Spring MVC 的异步执行器（会和 SSE 长连接的异步分派抢线程）。
 *
 * <p>拒绝策略统一用 {@link ThreadPoolExecutor.CallerRunsPolicy}：队列满时回退到调用线程，
 * 宁可这一轮慢一点，也不静默丢消息或丢连接归属。
 *
 * @author simonking
 */
@Configuration(proxyBeanMethods = false)
public class SseExecutorConfig {

    @Bean(name = "sseFanoutExecutor")
    public ThreadPoolTaskExecutor sseFanoutExecutor() {
        return executor("sse-fanout-", 0, 0, 2000);
    }

    @Bean(name = "sseHeartbeatExecutor")
    public ThreadPoolTaskExecutor sseHeartbeatExecutor() {
        return executor("sse-heartbeat-", 0, 0, 200);
    }

    /**
     * 归属写入是低频短任务，单线程足够；队列给足，避免建连高峰时回退同步
     */
    @Bean(name = "sseOwnerExecutor")
    public ThreadPoolTaskExecutor sseOwnerExecutor() {
        return executor("sse-owner-", 1, 2, 10000);
    }

    /**
     * Redis 消息分发：默认的 {@code SimpleAsyncTaskExecutor} 每条消息新建一个线程，
     * 高频推送下线程创建/销毁本身就是瓶颈
     */
    @Bean(name = "sseRedisListenerExecutor")
    public ThreadPoolTaskExecutor sseRedisListenerExecutor() {
        return executor("sse-redis-listener-", 2, 4, 10000);
    }

    /**
     * 订阅握手单独一个池：订阅阻塞时不能把消息分发一起卡住
     */
    @Bean(name = "sseRedisSubscriptionExecutor")
    public ThreadPoolTaskExecutor sseRedisSubscriptionExecutor() {
        return executor("sse-redis-subscription-", 1, 2, 1000);
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
