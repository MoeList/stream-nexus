package com.simonking.stream.nexus.sse.controller;

import com.simonking.stream.nexus.sse.auth.AdminAuthFilter;
import com.simonking.stream.nexus.sse.config.AdminAuthProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpSession;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 登录边界回归：会话固定防护与开放重定向
 *
 * @author simonking
 */
class LoginControllerTest {

    private final LoginController controller = new LoginController(new AdminAuthProperties());

    /**
     * 已存在会话时再次登录，必须先作废旧会话
     *
     * <p>否则 Spring Session 的 {@code RedisSessionRepository} 会对旧 Key 做 RENAME，
     * Redis Cluster 下新旧 sessionId 不在同一 slot，直接 CROSSSLOT 500
     */
    @Test
    void loginInvalidatesExistingSessionBeforeCreatingNewOne() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpSession old = new MockHttpSession();
        old.setAttribute("stale", "1");
        request.setSession(old);

        String view = controller.doLogin("admin", "adminsse", null, request);

        assertEquals("redirect:/admin", view);
        assertTrue(old.isInvalid(), "旧 session 必须先作废");
        assertNotSame(old, request.getSession(false));
        assertEquals("admin", request.getSession(false).getAttribute(AdminAuthFilter.SESSION_KEY));
    }

    @Test
    void safeRedirectRejectsBackslashAndProtocolRelative() {
        assertEquals("/admin", controller.safeRedirect("/\\evil.com"));
        assertEquals("/admin", controller.safeRedirect("//evil.com"));
        assertEquals("/admin", controller.safeRedirect("http://evil.com"));
        assertEquals("/admin", controller.safeRedirect("/admin\r\nLocation: http://evil.com"));
        assertEquals("/console", controller.safeRedirect("/console"));
    }
}
