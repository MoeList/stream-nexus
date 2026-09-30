package com.simonking.stream.nexus.sse.config;

import com.simonking.stream.nexus.sse.auth.ConnectAuthInterceptor;
import com.simonking.stream.nexus.sse.auth.PushAuthInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web MVC 配置
 *
 * @author simonking
 */
@Configuration
@RequiredArgsConstructor
public class WebMvcConfig implements WebMvcConfigurer {

    private final PushAuthInterceptor pushAuthInterceptor;

    private final ConnectAuthInterceptor connectAuthInterceptor;

    /**
     * 只对「终端接入面」开放跨域：订阅、心跳应答、业务推送
     *
     * <p>这三个入口本来就是给浏览器跨源调用的（{@code EventSource} / {@code fetch}），
     * 放开源、方法、头、凭证即可，鉴权仍由 {@link PushAuthInterceptor} 与建连令牌负责。
     *
     * <p><b>不要</b>再注册 {@code /**}：{@code /sse/admin/**} 的响应里有明文 apiKey，
     * 简单 GET 不触发预检，浏览器会直接放行跨源读取——管理员登录后，任意站点都能把
     * 推送凭证读走。管理页与运维接口必须保持同源。
     *
     * <p>用 {@code allowedOriginPatterns("*")} 而非 {@code allowedOrigins("*")}：后者在
     * {@code allowCredentials(true)} 下不接受 {@code *}，配置校验直接失败。
     * 自定义头（{@code X-Sse-AppId} / {@code X-Sse-Key}）必须放开，否则推送预检会被判失败。
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        cors(registry, "/sse/subscribe");
        cors(registry, "/sse/pong");
        cors(registry, "/sse/push");
    }

    private void cors(CorsRegistry registry, String pathPattern) {
        registry.addMapping(pathPattern)
                .allowedOriginPatterns("*")
                .allowedMethods("*")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }

    /**
     * 两条鉴权链各管一段：{@code /sse/push/**} 验应用凭证（谁能推），
     * {@code /sse/subscribe} 验建连令牌（谁能连）；后者默认关闭，见 {@link ConnectAuthInterceptor}
     */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(pushAuthInterceptor).addPathPatterns("/sse/push/**");
        registry.addInterceptor(connectAuthInterceptor).addPathPatterns("/sse/subscribe");
    }

    /**
     * 0 = 异步请求不过期，SSE 长连接由 {@code HeartbeatTask} 自行回收
     */
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(0L);
    }
}
