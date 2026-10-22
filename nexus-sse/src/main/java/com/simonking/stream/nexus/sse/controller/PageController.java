package com.simonking.stream.nexus.sse.controller;

import com.simonking.stream.nexus.sse.config.ClusterProperties;
import com.simonking.stream.nexus.sse.config.SseProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 页面跳转（Thymeleaf）
 *
 * <p>只负责「渲染哪个页面、往页面里塞什么服务端参数」，业务数据一律由页面自己调
 * {@code /sse/**} 接口拿——页面不该分担推送服务的状态查询职责。
 *
 * <p>默认页是管理页：运维是本服务最常打开的一屏，推送测试页退居 {@code /console}。
 *
 * <p>注意路径别与 {@code /sse/**} 冲突：这里是 {@code /admin}、{@code /console}，
 * 运维接口在 {@link AdminController} 的 {@code /sse/admin/**}。
 *
 * @author simonking
 */
@Controller
@RequiredArgsConstructor
public class PageController {

    private final SseProperties properties;

    /**
     * 运行模式开关：页面顶栏要标「单机 / 集群」，判定口径必须与 Bean 装配同源——
     * 集群相关 Bean 全部按 {@code nexus.sse.cluster.enabled=true} 条件装配，
     * 所以直接读配置类，而不是去探测某个 Bean 是否存在（后者在切片测试里容易失真）
     */
    private final ClusterProperties clusterProperties;

    /**
     * 根路径直达管理页
     */
    @GetMapping("/")
    public String home() {
        return "redirect:/admin";
    }

    @GetMapping("/admin")
    public String admin(Model model) {
        fill(model, "admin", "SSE 连接管理");
        return "admin";
    }

    @GetMapping("/console")
    public String console(Model model) {
        fill(model, "console", "SSE 推送测试");
        return "console";
    }

    /**
     * 服务端参数：心跳节奏、连接上限与运行模式由配置决定，页面不写死，
     * 避免配置改了页面还显示旧值
     */
    private void fill(Model model, String active, String title) {
        model.addAttribute("active", active);
        model.addAttribute("title", title);
        model.addAttribute("heartbeatInterval", properties.getHeartbeatInterval().toSeconds());
        model.addAttribute("heartbeatTimeout", properties.getHeartbeatTimeout().toSeconds());
        model.addAttribute("maxConnections", properties.getMaxConnections());
        // 运行模式：由启动参数决定，页面生命周期内不会变，故服务端直接渲染，
        // 不等 /sse/admin/** 接口回来再打标（那会先闪一帧错误标识）
        model.addAttribute("cluster", clusterProperties.isEnabled());
        // 下发给页面：开启建连鉴权时，推送测试页必须在连接前拦住没填令牌的情况，
        // 否则 EventSource 只会拿到一个 401 然后无脑重连，页面上看不出原因
        model.addAttribute("connectAuthEnabled", properties.isConnectAuthEnabled());
    }
}
