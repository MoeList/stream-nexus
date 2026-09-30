package com.simonking.stream.nexus.sse.core;

import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 推送门面
 *
 * <p>具体广播策略由 {@link SseBroadcaster} 决定：单机直接本地扇出，集群先发布到 Redis。
 * 这里刻意不再持有 {@code registry} / {@code sender}，避免上层绕过广播策略直接扇出，
 * 也保证「单机 / 集群」只有一条推送路径。
 *
 * @author simonking
 */
@Component
@RequiredArgsConstructor
public class SsePusher {

    private final SseBroadcaster broadcaster;

    /**
     * 推送一条消息
     *
     * <p>{@code bizModule} 缺省已在
     * {@link com.simonking.stream.nexus.sse.controller.PushController} 归一化为 {@code *}
     * （纯定向除外），因此这里拿到的 {@code bizModule} 为空时一定是一次纯定向推送。
     */
    public PushResult push(PushRequest request) {
        return broadcaster.push(request);
    }
}
