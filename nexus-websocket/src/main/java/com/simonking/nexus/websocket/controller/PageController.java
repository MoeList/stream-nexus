package com.simonking.nexus.websocket.controller;

import com.simonking.nexus.websocket.config.WsClusterProperties;
import com.simonking.nexus.websocket.config.WsProperties;
import com.simonking.stream.nexus.common.constant.WsConstants;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 页面跳转
 *
 * <p>服务端的运行时参数（端口、心跳节奏、鉴权开关）一律通过 Model 下发，
 * 页面里不写死任何常量，改配置即生效。
 *
 * @author simonking
 */
@Controller
@RequiredArgsConstructor
public class PageController {

    private final WsProperties properties;

    /**
     * 运行模式开关：页面顶栏要标「单机 / 集群」，判定口径必须与 Bean 装配同源——
     * 集群相关 Bean 全部按 {@code nexus.ws.cluster.enabled=true} 条件装配，
     * 所以直接读配置类，而不是去探测某个 Bean 是否存在（后者在切片测试里容易失真）
     */
    private final WsClusterProperties clusterProperties;

    @GetMapping("/")
    public String home() {
        return "redirect:/admin";
    }

    /**
     * 连接管理页（默认页）
     */
    @GetMapping("/admin")
    public String admin(Model model) {
        fill(model, "admin", "WebSocket 连接管理");
        return "admin";
    }

    /**
     * 推送测试页
     */
    @GetMapping("/console")
    public String console(Model model) {
        fill(model, "console", "WebSocket 推送测试");
        return "console";
    }

    private void fill(Model model, String active, String title) {
        model.addAttribute("active", active);
        model.addAttribute("title", title);
        model.addAttribute("wsPort", properties.getWsPort());
        model.addAttribute("wsPath", properties.getWsPath());
        // TCP 接入端口：管理页要同时展示两条链的端口（终端 WS / 业务系统 TCP）
        model.addAttribute("tcpPort", properties.getTcpPort());
        model.addAttribute("pushPath", WsConstants.PUSH_PATH);
        model.addAttribute("publicEndpoint", properties.getPublicEndpoint());
        model.addAttribute("heartbeatInterval", properties.getHeartbeatInterval().toSeconds());
        model.addAttribute("heartbeatTimeout", properties.getHeartbeatTimeout().toSeconds());
        model.addAttribute("maxConnections", properties.getMaxConnections());
        model.addAttribute("connectAuthEnabled", properties.isConnectAuthEnabled());
        model.addAttribute("authEnabled", properties.isAuthEnabled());
        // 运行模式：由启动参数决定，页面生命周期内不会变，故服务端直接渲染，
        // 不等 /ws/admin/** 接口回来再打标（那会先闪一帧错误标识）
        model.addAttribute("cluster", clusterProperties.isEnabled());
    }
}
