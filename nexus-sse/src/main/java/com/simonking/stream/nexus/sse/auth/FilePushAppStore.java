package com.simonking.stream.nexus.sse.auth;

import com.simonking.stream.nexus.sse.config.SseProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 文件台账：{@code appId / apiKey / 模块白名单} 落到一个本地 JSON 文件（单机模式）
 *
 * <p>为什么是文件而不是 DB：推送应用是**低频写、高频读**的小台账（几条到几十条），
 * 且单机部署不该为了这几行数据引入额外存储。文件足够，且天然可读可备份
 * （人工改完重启即生效，排障时也能直接看）。
 *
 * <p>代价要说清：**只适用于单实例**。多实例各写各的本地文件会互相覆盖认知，
 * 所以集群模式改用 {@link RedisPushAppStore}，{@link PushAppRegistry} 无感。
 *
 * <p>写文件用「临时文件 + 原子替换」：直接覆写遇到进程被 kill / 磁盘满会留下半个文件，
 * 下次启动解析失败，整张台账就没了。
 *
 * @author simonking
 */
@Component
@ConditionalOnProperty(name = "nexus.sse.cluster.enabled", havingValue = "false", matchIfMissing = true)
public class FilePushAppStore implements PushAppStore {

    private static final Logger log = LoggerFactory.getLogger(FilePushAppStore.class);

    private static final TypeReference<List<PushApp>> APP_LIST = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;

    private final Path file;

    public FilePushAppStore(SseProperties properties, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        // 存绝对路径：日志与页面提示里显示的是「真实写在哪」，相对路径容易让人找错地方
        this.file = resolve(properties.getAppStorePath());
    }

    /**
     * 台账路径解析：绝对路径原样采用；相对路径挂到模块根目录（nexus-sse）下
     */
    public static Path resolve(String configured) {
        Path p = Path.of(configured);
        Path base = p.isAbsolute() ? p : moduleRoot().resolve(p);
        return base.toAbsolutePath().normalize();
    }

    /**
     * 模块根目录（nexus-sse）：由本类所在的代码位置反推
     *
     * <p>IDEA / {@code spring-boot:run} 跑：{@code nexus-sse/target/classes} → 上两级 = {@code nexus-sse}；
     * 打成可执行 jar 跑：{@code nexus-sse/target/xxx.jar} → 上两级同上是 {@code nexus-sse}。
     *
     * <p>推导不出来时退回进程工作目录（行为与改动前一致）。但要注意：
     * 可执行 jar 下 CodeSource 是 {@code jar:file:/...!/BOOT-INF/classes!/}，
     * 直接交给 {@code Path.of(URI)} 会抛异常而不是「推导到一半」——不特殊处理就会一直退化到工作目录，
     * 同一个配置在不同启动目录下读写不同的台账，且不报任何错。
     */
    private static Path moduleRoot() {
        Path loc = codeSourcePath();
        if (loc != null) {
            Path up = loc.getParent();                       // target
            Path root = up == null ? null : up.getParent();  // nexus-sse
            if (root != null) {
                return root;
            }
        }
        return Path.of(System.getProperty("user.dir"));
    }

    /**
     * 本类代码所在的位置：目录（IDE）或 jar 文件（可执行 jar）
     *
     * <p>实测到三种写法，都不能直接交给 {@code Path.of(URI)}：
     * <ul>
     *     <li>IDE：{@code file:/.../nexus-sse/target/classes/}；</li>
     *     <li>可执行 jar：{@code /nested:/.../app.jar/!BOOT-INF/classes/!}（Spring Boot 4 实际长这样，
     *         开头多一个斜杠）；</li>
     *     <li>旧式写法：{@code jar:file:/.../app.jar!/BOOT-INF/classes!/}。</li>
     * </ul>
     *
     * <p>所以分两步：先砍掉 {@code .jar} 之后的部分（那里是嵌套路径，不是文件系统路径），
     * 再从剩下的字符串里取出「真正的文件路径」——Windows 取最后一个盘符开始，
     * 其它情况取 {@code file:} 之后。只要拿到 jar（或 classes 目录），上两级就是模块根。
     *
     * @return 推导不出来时返回 {@code null}
     */
    private static Path codeSourcePath() {
        try {
            CodeSource src = FilePushAppStore.class.getProtectionDomain().getCodeSource();
            if (src == null || src.getLocation() == null) {
                return null;
            }
            return toPath(filePathOf(src.getLocation().toString()));
        } catch (Exception e) {
            // 退回工作目录；留一行 debug 便于定位「台账怎么写到别处去了」
            log.debug("[sse] 解析代码位置失败，台账路径退回工作目录：{}", e.toString());
            return null;
        }
    }

    /**
     * 从代码位置串里取出文件系统路径：先砍掉 {@code .jar} 之后的嵌套部分，
     * 再剥离 Spring Boot 4 的 {@code nested:} 伪协议，最后定位真正的路径起点
     *
     * <p>单独抽成纯字符串方法是为了能单测：这串写法随启动方式变化（IDE / 可执行 jar / 旧式 jar:），
     * 而它错了不会报错，只会静默把台账读写到别处。
     */
    static String filePathOf(String raw) {
        int jar = raw.toLowerCase().indexOf(".jar");
        if (jar > 0) {
            raw = raw.substring(0, jar + 4);
        }
        // Spring Boot 4 可执行 jar：/nested:/opt/app.jar（Windows 是 /nested:/E:/...）。
        // nested: 不是文件系统路径的一部分，不剥掉的话 Linux 下会得到 /nested:/opt/app.jar，
        // moduleRoot() 上两级就成了 /nested:，台账直接写到根目录的同名目录
        int nested = raw.indexOf("nested:");
        if (nested >= 0) {
            raw = raw.substring(nested + "nested:".length());
        }
        int drive = lastDriveIndex(raw);
        if (drive >= 0) {
            return raw.substring(drive);
        }
        String prefix = "file:";
        int at = raw.indexOf(prefix);
        return at >= 0 ? raw.substring(at + prefix.length()) : raw;
    }

    /**
     * 文件路径 → {@link Path}
     *
     * <p>走 URI 是为了还原 {@code %20} 之类的转义；路径里若含空格等未转义字符 URI 会构造失败，
     * 此时退回直接按本地路径解析（Windows 同样认 {@code /} 分隔），不能因为路径带空格就退到工作目录。
     */
    static Path toPath(String path) {
        String uri = path.startsWith("/") ? "file:" + path : "file:/" + path;
        try {
            return Path.of(URI.create(uri));
        } catch (IllegalArgumentException e) {
            return Path.of(path);
        }
    }

    /**
     * 最后一个 Windows 盘符（{@code X:/} 或 {@code X:\}）的位置，找不到返回 -1
     *
     * <p>取最后一个而不是第一个：{@code nested:} 这类前缀里也有「字母 + 冒号 + 斜杠」，排在真正的盘符之前。
     * 反过来又得排除 {@code file:/opt} 末尾的 {@code e:/}——靠「紧邻前面是字母就不算盘符」区分。
     */
    private static int lastDriveIndex(String raw) {
        for (int i = raw.length() - 3; i >= 0; i--) {
            char c = raw.charAt(i);
            if (Character.isLetter(c) && raw.charAt(i + 1) == ':'
                    && (raw.charAt(i + 2) == '/' || raw.charAt(i + 2) == '\\')
                    && (i == 0 || !Character.isLetter(raw.charAt(i - 1)))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 持久化文件的绝对路径
     */
    public Path path() {
        return file;
    }

    @Override
    public String describe() {
        return file.toString();
    }

    /**
     * 文件是否已存在
     *
     * <p>与 {@link #load()} 返回空列表区分开：文件存在但列表为空 = 用户把应用删光了，
     * 这时**不能**重新播种默认应用，否则「删光后重启又被塞回一个 test」很反直觉。
     */
    @Override
    public boolean exists() {
        return Files.isRegularFile(file);
    }

    /**
     * 读回台账；文件不存在 / 为空 → 空列表；解析失败 → 备份原文件后返回空列表
     */
    @Override
    public List<PushApp> load() {
        if (!exists()) {
            return List.of();
        }
        String json;
        try {
            json = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("推送应用台账读取失败，按空台账启动：{}", file, e);
            return List.of();
        }
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            List<PushApp> apps = objectMapper.readValue(json, APP_LIST);
            return apps == null ? List.of() : apps;
        } catch (JacksonException e) {
            // 坏文件不直接丢：改名留档，人工还能抢救；同时避免下次启动反复报错
            log.error("推送应用台账不是合法 JSON，已备份为 .bad 并以空台账启动：{}", file, e);
            quarantine();
            return List.of();
        }
    }

    @Override
    public void save(PushApp app) {
        List<PushApp> current = new ArrayList<>(load());
        current.removeIf(item -> Objects.equals(item.appId(), app.appId()));
        current.add(app);
        write(current);
    }

    @Override
    public boolean delete(String appId) {
        List<PushApp> current = new ArrayList<>(load());
        boolean removed = current.removeIf(item -> Objects.equals(item.appId(), appId));
        if (removed) {
            write(current);
        }
        return removed;
    }

    /**
     * 文件台账没有跨节点版本概念：单机只有一个写者
     */
    @Override
    public long version() {
        return -1L;
    }

    /**
     * 原子写回全量台账
     *
     * @throws IllegalStateException 写盘失败：内存已改而磁盘没落，属于必须让人知道的错误
     */
    private void write(List<PushApp> apps) {
        try {
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            Files.writeString(tmp, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(apps),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                // 部分文件系统 / 跨卷不支持原子移动，退化成普通替换（仍比直接覆写安全）
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("推送应用台账落盘失败（内存已生效，重启会丢失）：" + file, e);
        }
    }

    /**
     * 把解析失败的文件改名留档（{@code xxx.json.bad}），不直接删
     */
    private void quarantine() {
        try {
            Files.move(file, file.resolveSibling(file.getFileName() + ".bad"),
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.warn("备份损坏的推送应用台账失败：{}", file, e);
        }
    }
}
