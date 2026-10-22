package com.simonking.nexus.websocket.core;

import com.simonking.stream.nexus.common.model.PushRequest;
import com.simonking.stream.nexus.common.model.PushResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 推送门面
 *
 * <p>只做一件事：把请求交给当前的广播策略（{@link WsBroadcaster}）。
 * 真正的寻址与扇出在 {@link AbstractWsBroadcaster} 里，单机 / 集群的差异在
 * {@link LocalWsBroadcaster} 与 {@code RedisWsBroadcaster} 里。
 *
 * <p>保留这个门面而不是让 {@code PushService} / {@code TcpPushService} 直接注入接口，
 * 是为了让「推送」在两个入口处保持同一个可读的名字，也方便将来在门面层加统一的
 * 埋点或限流，而不必改两个调用方。
 *
 * <p>改造前的门面自己持有注册表并在「本地没有目标连接」时提前返回。这个短路在集群模式下
 * 会造成跨节点漏推（业务系统把消息交给 A 节点、目标终端连在 B 节点），已经去掉：
 * 现在无论本地是否命中，都先按当前模式完成广播，再本地扇出。
 *
 * @author simonking
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class WsPusher {

    private final WsBroadcaster broadcaster;

    /**
     * 推送消息
     */
    public PushResult push(PushRequest request) {
        return broadcaster.push(request);
    }
}
