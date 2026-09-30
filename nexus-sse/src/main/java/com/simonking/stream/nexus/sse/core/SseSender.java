package com.simonking.stream.nexus.sse.core;

import tools.jackson.databind.ObjectMapper;
import com.simonking.stream.nexus.common.enums.SseEvent;
import com.simonking.stream.nexus.common.model.NexusMessage;
import com.simonking.stream.nexus.sse.connection.SseClient;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;

/**
 * 单条消息写入
 *
 * <p>消息体预先序列化为 JSON 字符串再交给 SSE，避免每个连接重复一次对象转换。
 *
 * @author simonking
 */
@Component
@RequiredArgsConstructor
public class SseSender {

    private final JsonMapper jsonMapper;

    /**
     * 向指定连接发送消息
     *
     * @throws IOException 连接已失效或写入阻塞失败
     */
    public void send(SseClient client, NexusMessage<?, SseEvent> message) throws IOException {
        SseEmitter.SseEventBuilder builder = SseEmitter.event();
        if (message.getId() != null) {
            builder.id(message.getId());
        }
        builder.name(message.getEvent().name())
                .data(jsonMapper.writeValueAsString(message), MediaType.APPLICATION_JSON);
        client.getEmitter().send(builder);
    }

    /**
     * 发送已经序列化好的消息 JSON
     *
     * <p>集群消费端拿到的 {@code ClusterMessage.message} 就是完整 JSON，
     * 直接写 SSE 即可：不再反序列化成对象再序列化，既保留原始类型，也省一次转换。
     *
     * @param event     SSE 事件名（{@code MESSAGE} / {@code PING} 等）
     * @param messageId 消息ID，可为空（心跳消息不带 id）
     * @param json      完整 {@link NexusMessage} JSON
     */
    public void sendRaw(SseClient client, String event, String messageId, String json) throws IOException {
        SseEmitter.SseEventBuilder builder = SseEmitter.event();
        if (messageId != null && !messageId.isBlank()) {
            builder.id(messageId);
        }
        String eventName = (event == null || event.isBlank()) ? SseEvent.MESSAGE.name() : event;
        builder.name(eventName).data(json, MediaType.APPLICATION_JSON);
        client.getEmitter().send(builder);
    }
}
