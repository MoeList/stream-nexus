package com.simonking.stream.nexus.sse;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.session.SessionRepository;
import org.springframework.session.web.http.SessionRepositoryFilter;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class NexusSseApplicationTests {

    @Autowired
    private ListableBeanFactory beanFactory;

    @Test
    void contextLoads() {
    }

    /**
     * 单机模式不得出现任何 Spring Session Bean
     *
     * <p>session 依赖在 classpath 上最容易出的错，就是「单机悄悄启用了 Redis 会话」：
     * 本地没 Redis 也能启动，直到有人登录才报错。把门槛前移到上下文启动时。
     */
    @Test
    void standaloneDoesNotEnableSpringSession() {
        assertEquals(0, beanFactory.getBeanNamesForType(SessionRepository.class, true, false).length);
        assertEquals(0, beanFactory.getBeanNamesForType(SessionRepositoryFilter.class, true, false).length);
    }
}
