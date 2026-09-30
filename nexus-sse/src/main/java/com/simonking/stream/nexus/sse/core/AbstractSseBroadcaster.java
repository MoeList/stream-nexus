package com.simonking.stream.nexus.sse.core;

import com.simonking.stream.nexus.common.cluster.ClusterMessage;
import com.simonking.stream.nexus.common.cluster.ClusterMessageType;
import com.simonking.stream.nexus.common.constant.NexusConstants;
import com.simonking.stream.nexus.common.enums.SseEvent;
import com.simonking.stream.nexus.common.model.NexusMessage;
import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
import com.simonking.stream.nexus.common.util.IdGenerator;
import com.simonking.stream.nexus.common.util.NexusUtils;
import com.simonking.stream.nexus.sse.connection.SseClient;
import com.simonking.stream.nexus.sse.connection.SseClientRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tools.jackson.databind.ObjectMapper;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

/**
 * 广播器公共部分：消息构建、目标解析、本地扇出
 *
 * <p>单机与集群只在「是否先发布到 Redis」上分叉，扇出逻辑必须只有一份——
 * 否则两侧的模块路由、全局模块叠加、失败回收稍有出入，就会出现
 * 「单机推得到、集群推不到」这类最难排查的差异。
 *
 * @author simonking
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractSseBroadcaster implements SseBroadcaster {

    protected final SseClientRegistry registry;

    protected final SseSender sender;

    protected final ObjectMapper objectMapper;

    protected final Executor fanoutExecutor;

    /**
     * 构建一条推送消息，并预先序列化成 JSON
     *
     * <p>消息 ID 在入口节点生成一次，所有节点共用；{@code originNodeId} 用于消费端跳过回环。
     */
    protected ClusterMessage buildMessage(PushRequest request) {
        NexusMessage<Object, SseEvent> message = NexusMessage.<Object, SseEvent>builder()
                .id(IdGenerator.nextId())
                .event(SseEvent.MESSAGE)
                .bizModule(request.getBizModule())
                .action(request.getAction())
                .ts(System.currentTimeMillis())
                .data(request.getData())
                .build();

        return ClusterMessage.builder()
                .type(ClusterMessageType.PUSH)
                .originNodeId(originNodeId())
                .messageId(message.getId())
                .bizModule(request.getBizModule())
                .clientIds(request.getClientIds())
                .event(message.getEvent().name())
                .action(message.getAction())
                .ts(message.getTs())
                .message(objectMapper.writeValueAsString(message))
                .build();
    }

    /**
     * 当前节点ID：单机返回 null（不参与序列化），集群返回节点身份
     */
    protected abstract String originNodeId();

    /**
     * 本地扇出
     *
     * <p>各连接之间无顺序依赖，用专用线程池分片并行写入，避免单条慢连接拖住整批；
     * 不用 {@code parallelStream()}：那是 ForkJoinPool.commonPool，会被 JVM 内其它并行流拖慢，
     * 也无法限制队列长度。计数用 {@link LongAdder}，高并发累加竞争更小。
     *
     * <p>写入失败的连接立即回收，避免无效连接长期占用内存与文件句柄。
     */
    @Override
    public PushResult pushLocal(ClusterMessage message) {
        Collection<SseClient> targets = resolveTargets(message.getBizModule(), message.getClientIds());

        LongAdder success = new LongAdder();
        LongAdder failed = new LongAdder();
        ParallelTasks.forEach(targets, fanoutExecutor, client -> sendOne(client, message, success, failed));

        return PushResult.builder()
                .messageId(message.getMessageId())
                .total(targets.size())
                .success(success.intValue())
                .failed(failed.intValue())
                .build();
    }

    private void sendOne(SseClient client, ClusterMessage message, LongAdder success, LongAdder failed) {
        try {
            sender.sendRaw(client, message.getEvent(), message.getMessageId(), message.getMessage());
            success.increment();
        } catch (Exception e) {
            failed.increment();
            log.warn("[sse] send failed, clientId={}, bizModule={}, msgId={}",
                    client.getClientId(), message.getBizModule(), message.getMessageId());
            registry.remove(client.getClientId());
        }
    }

    /**
     * 解析目标连接：模块命中 ∪ 全局模块订阅者 ∪ 定向命中，按 clientId 去重
     *
     * <p>规则与改造前的 {@code SsePusher.resolveTargets} 完全一致：
     * <ul>
     *     <li>推送目标是 {@code *} → 广播给全部在线连接；</li>
     *     <li>推送目标是其它模块 → 额外带上订阅了 {@code *} 的连接；</li>
     *     <li>纯定向推送（只填 clientIds）不叠加全局模块，避免一对一消息扩散。</li>
     * </ul>
     */
    protected Collection<SseClient> resolveTargets(String bizModule, List<String> clientIds) {
        boolean byModule = StringUtils.hasText(bizModule);
        boolean byClient = !CollectionUtils.isEmpty(clientIds);

        if (!byModule && !byClient) {
            return List.of();
        }

        Map<String, SseClient> result = new LinkedHashMap<>();

        if (byModule) {
            Collection<SseClient> moduleClients = NexusUtils.isGlobal(bizModule)
                    ? registry.all()
                    : registry.byModule(bizModule);
            for (SseClient client : moduleClients) {
                result.putIfAbsent(client.getClientId(), client);
            }
            for (SseClient client : registry.byModule(NexusConstants.GLOBAL_MODULE)) {
                result.putIfAbsent(client.getClientId(), client);
            }
        }

        if (byClient) {
            for (SseClient client : registry.byClientIds(clientIds)) {
                result.putIfAbsent(client.getClientId(), client);
            }
        }
        return result.values();
    }
}
