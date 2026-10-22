package com.simonking.stream.nexus.common.util;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 节点位配置防护
 *
 * <p>节点位是静态状态：同一 JVM 内出现第二个「节点身份」时如果不拦住，
 * 两个节点会静默生成同一批 ID，事后无法追溯是哪一边重复了。
 * 这里按顺序验证：首次配置生效 → 相同值幂等 → 不同值失败关闭 → 越界拒绝。
 *
 * <p>用例顺序固定：静态状态不可重置，换序会让「首次配置」断言失去意义。
 *
 * @author simonking
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IdGeneratorTest {

    @Test
    @Order(1)
    void configureNodeAppliesNodeBits() {
        IdGenerator.configureNode(7);

        assertEquals(7, IdGenerator.node());
    }

    @Test
    @Order(2)
    void configureNodeIsIdempotentForSameValue() {
        assertDoesNotThrow(() -> IdGenerator.configureNode(7));

        assertEquals(7, IdGenerator.node());
    }

    @Test
    @Order(3)
    void configureNodeRejectsDifferentValueInSameJvm() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> IdGenerator.configureNode(8));

        assertTrue(ex.getMessage().contains("只允许一个节点身份"), ex.getMessage());
        assertEquals(7, IdGenerator.node());
    }

    @Test
    @Order(4)
    void configureNodeRejectsOutOfRangeValue() {
        assertThrows(IllegalArgumentException.class, () -> IdGenerator.configureNode(-1));
        assertThrows(IllegalArgumentException.class, () -> IdGenerator.configureNode(4096));
    }

    @Test
    @Order(5)
    void nextIdIsStrictlyIncreasing() {
        String previous = IdGenerator.nextId();
        for (int i = 0; i < 5000; i++) {
            String current = IdGenerator.nextId();
            assertTrue(Long.parseLong(current) > Long.parseLong(previous), "ID 必须严格递增");
            previous = current;
        }
    }
}
