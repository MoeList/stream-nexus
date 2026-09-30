package com.simonking.nexus.websocket.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * 分片并行执行
 *
 * <p>不按「每个元素一个任务」提交：上万条连接会产生上万个 Future，对象分配与队列竞争
 * 比发送本身还贵。按 CPU 数把元素切成若干片，一片一个任务，既保留并行度，也把
 * 任务数控制在几十个。
 *
 * <p>调用线程会等待全部片完成（{@code allOf().join()}），保证 {@code pushLocal} 返回的
 * success / failed 是准确的，心跳也不会出现「上一轮还没发完、下一轮又进来」的重叠。
 *
 * <p>与 {@code nexus-sse} 的 {@code core/ParallelTasks} 同构：两个服务互不依赖，
 * 为一个几十行、零依赖的工具类引入公共模块的耦合不划算，因此各留一份。
 *
 * @author simonking
 */
public final class ParallelTasks {

    private static final int MAX_PARALLELISM = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);

    private ParallelTasks() {
    }

    public static <T> void forEach(Collection<T> items, Executor executor, Consumer<T> action) {
        if (items == null || items.isEmpty()) {
            return;
        }
        List<T> list = new ArrayList<>(items);
        int workers = Math.min(MAX_PARALLELISM, list.size());
        int chunkSize = (list.size() + workers - 1) / workers;

        List<CompletableFuture<Void>> futures = new ArrayList<>(workers);
        for (int from = 0; from < list.size(); from += chunkSize) {
            List<T> chunk = list.subList(from, Math.min(from + chunkSize, list.size()));
            futures.add(CompletableFuture.runAsync(() -> {
                for (T item : chunk) {
                    action.accept(item);
                }
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}
