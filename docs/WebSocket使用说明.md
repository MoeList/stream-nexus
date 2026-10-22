# WebSocket 推送服务使用说明

> 模块：`nexus-websocket` · 默认端口：WebSocket `9090` / HTTP `8089` / TCP `9091` · 技术栈：Spring Boot 4.1.1 / Java 17 / Netty 4.2 · 支持可选 Redis 集群模式

`nexus-websocket` 与 SSE 通道定位互补：SSE 是「HTTP 生态、单向、浏览器原生重连」，WebSocket 是「全双工、可上行、适合高频交互」。两者**协议与端口完全独立**，可同时部署。

设计原型：WebSocket 服务独立部署，业务系统不写任何 Netty / WebSocket 服务端代码，而是通过「REST 接口（或 TCP 长连接）」把消息交给它，由它转发给终端。

---

## 架构与端口

```
   浏览器 / H5 / App
          │  ws://host:9090/ws?modules=test
          ▼
   nexus-websocket（独立部署，一个进程三组端口）
     ├── 9090  Netty WebSocket —— 终端长连接
     ├── 9091  Netty TCP       —— 业务系统长连接（连上即可推，只对内网开放）
     └── 8089  HTTP            —— REST 推送入口 / 管理界面 / 测试页 / 运维接口
          ▲
          │  HTTP POST /ws/push（短连接）或 TCP 长连接（9091）
   业务系统 ← nexus-client（或直接 HTTP 调用）
```

Netty 与 Spring MVC 在**同一进程内共享连接注册表**：REST / TCP 收到推送 → 查注册表 → 直接写 WebSocket 通道。

---

## 启动与验证

```bash
mvn -pl nexus-websocket -am package -DskipTests
java -jar nexus-websocket/target/nexus-websocket-2.0.0.jar
```

或使用仓库自带 wrapper：

```bash
mvnw.cmd -pl nexus-websocket -am package -DskipTests   # Windows
./mvnw -pl nexus-websocket -am package -DskipTests     # Linux / macOS
```

验证：

1. 打开 `http://localhost:8089/console` 推送测试页，默认订阅模块 `test`，点击「连接」。
2. 选择默认应用 `test-demo` / `c3RyZWFtLW5leHVz`，点「推送」→ 页面收到 `test:bid` 消息。
3. 打开 `http://localhost:8089/admin` 查看在线连接台账、模块分布、强制下线、应用管理。

---

## 集群模式（可选）

默认 `nexus.ws.cluster.enabled=false`：单机运行，不建立任何 Redis 连接，行为与改造前一致。
多节点部署时打开开关，节点通过 Redis 共享推送通道、节点台账、应用台账与登录态：

```properties
# 本地自建 application-cluster.properties（已加入 .gitignore），用 --spring.profiles.active=cluster 激活
nexus.ws.cluster.enabled=true
nexus.ws.cluster.node-id=node-1          # 集群内唯一
nexus.ws.cluster.node-seq=1              # 0~4095，集群内唯一
nexus.ws.cluster.key-prefix=nexus:ws     # 与 SSE 共用同一个 Redis 时必须区分
nexus.ws.cluster.redis-protocol=resp2    # Redis 3.x 必须降级，否则订阅注册超时
spring.data.redis.cluster.nodes=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
# 单节点 Redis 用这一组，不要再配 cluster.nodes：
# spring.data.redis.host=127.0.0.1
# spring.data.redis.port=6379
```

| 能力 | 实现 |
| --- | --- |
| 跨节点推送 | Redis Pub/Sub `{key-prefix}:bus:push`；REST 与 TCP 收到的消息都经总线扇出，按 `originNodeId` 跳过回环 |
| 节点注册 | Hash `{key-prefix}:nodes`，每 5s 续期；启动做 nodeId / nodeSeq 唯一性校验，冲突直接拒绝启动 |
| 应用台账 | Hash `{key-prefix}:apps` + `:apps:version`；改动广播 `APPS_CHANGED`，其他节点立即重载 |
| 连接台账 | 管理页按「请求-回复」向各节点收集快照（`SNAPSHOT_REQUEST` → Redis List），超时返回 `partial=true` + 缺失节点 |
| 踢下线 | `KICK` / `KICK_ALL` 控制消息广播，命中节点本地关闭连接 |
| 管理页登录态 | Spring Session Redis，键空间 `{key-prefix}:session`；任一节点登录全集群可用 |

本地起两个副本的示例（端口必须错开）：

```bash
java -jar nexus-websocket/target/nexus-websocket-2.0.0.jar --spring.profiles.active=cluster \
  --server.port=18089 --nexus.ws.ws-port=19090 --nexus.ws.tcp-port=19091 \
  --nexus.ws.cluster.node-id=node-1 --nexus.ws.cluster.node-seq=1

java -jar nexus-websocket/target/nexus-websocket-2.0.0.jar --spring.profiles.active=cluster \
  --server.port=18099 --nexus.ws.ws-port=19190 --nexus.ws.tcp-port=19191 \
  --nexus.ws.cluster.node-id=node-2 --nexus.ws.cluster.node-seq=2
```

要点：

1. **唯一性校验**：nodeId 双开或 nodeSeq 被在线节点占用时拒绝启动（宁可起不来，也不让两个节点撞消息 ID）。
2. **失败关闭**：开关打开但 Redis 连不上，进程直接启动失败（exit 1），不会静默退回单机。
3. **首次迁移**：`nexus.ws.cluster.apps-import-on-startup=true` 只开一次，把本地 `push-apps.json` 导入 Redis 台账。
4. **管理页台账**：各节点返回本节点快照（WS + TCP 一批返回），发起节点聚合；超时或节点未回复时页面标记 `partial` 并列出缺失节点。
5. **踢下线**：任意节点发起都有效，控制消息广播后由持有连接的节点本地关闭。
6. **入口无关**：终端与业务系统可连任意节点；REST / TCP 消息落到哪个节点，都会广播到全集群。

---

## REST 推送接口

### curl 示例

```bash
curl -X POST http://localhost:8089/ws/push \
  -H 'Content-Type: application/json' \
  -H 'X-Ws-AppId: test-demo' \
  -H 'X-Ws-Key: c3RyZWFtLW5leHVz' \
  -d '{"bizModule":"test","action":"bid","data":{"itemId":"L123"}}'
```

响应：

```json
{"messageId":"1758123456789-1","total":2,"success":2,"failed":0}
```

状态码：200 成功；400 请求体为空或 `bizModule` / `clientIds` 都为空；401 凭证不配对；403 模块不在白名单。

### Java 接入（nexus-client SDK）

```java
NexusRestClient client = NexusRestClient.builder()
        .wsBaseUrl("http://localhost:8089")
        .wsAppId("test-demo")
        .wsApiKey("c3RyZWFtLW5leHVz")
        .build();

PushResult result = client.wsPush(PushRequest.builder()
        .bizModule("test")
        .action("bid")
        .data(Map.of("itemId", "L123"))
        .build());
```

更多 SDK 用法见 [SDK 接入说明](SDK接入说明.md)。

---

## TCP 长连接接入通道（9091）

REST 推送是短连接，高频推送时每条消息都要重建 HTTP 连接、重走一次鉴权；TCP 通道让业务系统**一条长连接一直推**，还能靠心跳提前发现链路中断。

- **不做应用鉴权**：连接建立即可推送。TCP 端口与 REST 接口一样只对内网开放，「端口不暴露」就是它的边界。
- **帧格式**：`[4 字节大端长度][UTF-8 JSON]`。
- **报文体**：复用 `NexusMessage`，`event` 取 `TcpEvent`（`PUSH` / `RESULT` / `PING` / `PONG` / `ERROR`）。

业务系统侧不用自己写 Netty，`nexus-client` 里的 `NexusTcpClient` 已封装好：

```java
try (NexusTcpClient client = NexusTcpClient.builder()
        .host("10.0.0.8")
        .port(9091)
        .build()) {
    client.push(PushRequest.builder()
            .bizModule("order")
            .action("CREATE")
            .data(data)
            .build());
}
```

报文示例：

```json
{"event":"PUSH","data":{"bizModule":"test","action":"bid","data":{"itemId":"L123"}}}
```

回执示例：

```json
{"event":"RESULT","bizModule":"tcp","action":"result","ts":...,"data":{"messageId":"...","total":1,"success":1,"failed":0}}
```

规则：`bizModule` / `clientIds` 都为空等参数问题只回 `ERROR`、不关连接，改完报文可以接着推；心跳沿用 `nexus.ws.*`（写空闲 15s 服务端发 `PING`，读空闲 90s 未收到任何上行数据即判定失联并关闭）。

---

## 浏览器接入

```javascript
let clientId = '';
const ws = new WebSocket('ws://localhost:9090/ws?modules=test');

ws.onopen = () => {
  // 建连成功后必须重新拉取全量业务状态
  refreshAll();
};

ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.event === 'CONNECTED') {
    clientId = m.data.clientId;
    return;
  }
  if (m.event === 'PING') {
    ws.send(JSON.stringify({ event: 'PONG' }));
    return;
  }
  if (m.event === 'KICKED') {
    ws.close();
    return;
  }
  // 按 bizModule:action 路由
  const key = `${m.bizModule}:${m.action}:${m.data.itemId ?? ''}`;
  if (m.ts <= (lastTs[key] || 0)) return;
  lastTs[key] = m.ts;
  render(m);
};

ws.onclose = () => {
  // 浏览器需要自行实现重连
};
```

要点：

1. `clientId` 由服务端在握手时生成（UUID），随 `CONNECTED` 回执下发；前端不需要传任何标识。
2. **重连即换**：断开再连会拿到新 ID。要按用户维度稳定寻址，优先用**模块订阅**（`?modules=xxx`），或在业务系统侧维护「用户 → 当前 clientId」映射。
3. 浏览器 `WebSocket` 对象收不到协议层 ping/pong 帧，因此心跳必须是应用级消息：收到 `{"event":"PING"}` 必须回 `{"event":"PONG"}`，否则 90s 后被判定失联回收。
4. `ws.onopen` 必须重新拉取全量业务状态——服务端无快照、无补发。

---

## 测试页：自定义连接测外部 WebSocket

推送测试页 `http://localhost:8089/console` 的「WebSocket 连接」区提供**两种地址模式**，既可连本服务，也可用来测任意外部 WebSocket 链接：

| 模式 | 入口 | 地址来源 | 适用 |
| --- | --- | --- | --- |
| 系统地址（默认） | 不填地址框 | 自动把「订阅模块 + 建连令牌」拼进 `ws://host:9090/ws` | 连本服务自身，做冒烟验证 |
| 自定义连接地址 | 点「自定义连接地址」按钮 | 以地址框内容为准，点「连接」直接建连 | 测任意外部 WS 链接（如 `wss://host/ws`）的连通性与消息到达 |

要点：

1. 外部 WS 服务不认本服务的 `modules` 订阅、建连令牌与应用级上行消息约定，切到自定义模式后「订阅模块 / 建连令牌 / 上行消息」会**一并隐藏**，避免误导；点「重置地址」回到系统默认地址。
2. 与 SSE 不同，WebSocket 下行的每条都是完整 JSON，页面按报文里的 `event` 字段自行路由，**无需切换事件名**；因此测外部 WS 时只需换地址框，没有额外的监听配置。
3. 外部服务的消息体结构与本服务 `NexusMessage` 不同时，页面可能无法按 `bizModule:action` 识别与去重，仅能验证“收到字节”层面的连通性；若要完整展示需外部服务兼容同一消息协议。

---

## 管理界面

| 路径 | 说明 |
| --- | --- |
| `http://localhost:8089/admin` | 连接管理页：WS 在线台账、模块分布、强制下线、TCP 接入台账、推送应用管理 |
| `http://localhost:8089/console` | 推送测试页：建连、应答心跳、推送、看日志 |

运维接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/ws/admin/connections` | 在线连接概览（集群模式为全集群聚合） |
| DELETE | `/ws/admin/connections/{clientId}` | 强制下线（集群模式广播到所有节点） |
| DELETE | `/ws/admin/connections` | 全部强制下线（广播 `KICK_ALL`） |
| GET | `/ws/admin/tcp/connections` | TCP 接入连接台账（同样参与集群聚合） |
| GET | `/ws/admin/cluster/nodes` | 节点列表（单机只返回本节点） |
| GET | `/ws/admin/apps` | 推送应用列表 |
| POST | `/ws/admin/apps` | 新增应用 |
| PUT | `/ws/admin/apps/{appId}` | 修改应用白名单 |
| DELETE | `/ws/admin/apps/{appId}` | 删除应用 |

> WebSocket 推送应用**持久化**：内置默认应用 `test-demo / c3RyZWFtLW5leHVz`（白名单 `test`，**只读**：不可改、不可删），
> 其余应用在管理页「推送应用」页签运行时增删。单机模式写入 `nexus.ws.app-store-path`（默认 `nexus-websocket/data/push-apps.json`），
> 集群模式改存 Redis Hash `{key-prefix}:apps`，多节点共享、改动即时广播；管理页的「台账位置」字段会显示实际存储。
>
> 连接台账接口在集群模式下额外返回 `cluster`、`nodeId`、`partial`、`receivedNodes` / `expectedNodes` 与 `missingNodes`，
> 每行连接带 `protocol`（`WS` / `TCP`）与 `nodeId`；单机下 `cluster=false`、`partial=false`、行上没有 `nodeId`。
>
> `/ws/admin/**` 运维接口与 `/admin`、`/console` 页面**需 session 登录**（默认 `admin / adminws`，由 `nexus.ws.admin.*` 配置）；
> 未登录时页面 302 到 `/login`、接口返回 401 JSON。部署务必改掉默认口令，并限制内网访问。

---

## 主要配置

`nexus-websocket/src/main/resources/application.properties`：

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `server.port` | `8089` | HTTP / REST / 管理页端口 |
| `nexus.ws.ws-port` / `nexus.ws.ws-path` | `9090` / `/ws` | WebSocket 监听端口与握手路径 |
| `nexus.ws.tcp-port` | `9091` | TCP 接入端口（业务系统长连接） |
| `nexus.ws.heartbeat-interval` / `nexus.ws.heartbeat-timeout` | `15s` / `90s` | WS 心跳间隔与失联阈值 |
| `nexus.ws.max-connections` | `30000` | 最大 WS 连接数，0 = 不限 |
| `nexus.ws.max-frame-length` | `65536` | 单个 WebSocket 帧最大长度（字节） |
| `nexus.ws.ip2region-path` | `classpath:ip2region.xdb` | 台账 IP 归属地的离线库（xdb）；数据文件需自备，缺失时城市列显示 `-`，私有地址直接判为「局域网」 |
| `nexus.ws.boss-threads` / `nexus.ws.worker-threads` | `1` / `0` | Netty 线程数；0 = 取 Netty 默认值 |
| `nexus.ws.auth-enabled` | `true` | 推送鉴权（`X-Ws-AppId` + `X-Ws-Key`），**只作用于 REST 推送** |
| `nexus.ws.connect-auth-enabled` / `nexus.ws.connect-auth-token` | `false` / `U3RyZWFtTmV4dXM=` | 建连鉴权（查询参数 `token`），WebSocket 握手无法自定义请求头 |
| `nexus.ws.public-endpoint` | `` | 控制台页面拼接 WebSocket 地址时使用；留空则自动取当前 hostname + `nexus.ws.ws-port` |
| `nexus.ws.cluster.enabled` | `false` | 集群开关；`false` = 单机，不建立任何 Redis 连接 |
| `nexus.ws.cluster.node-id` / `node-seq` | 空 / 空 | 节点身份，集群内唯一；留空按 `host-ws-port` 与 nodeId 哈希兜底，生产请显式配置 |
| `nexus.ws.cluster.key-prefix` | `nexus:ws` | Redis Key / Channel 命名空间；与 SSE 共用同一个 Redis 时必须区分 |
| `nexus.ws.cluster.redis-protocol` | `resp3` | Redis 3.x 或只支持 RESP2 的代理必须设为 `resp2` |
| `nexus.ws.cluster.node-heartbeat-interval` / `node-timeout` | `5s` / `30s` | 节点心跳与离线判定 |
| `nexus.ws.cluster.admin-timeout` | `500ms` | 管理页聚合连接快照的等待上限，超时返回 `partial` |
| `nexus.ws.cluster.app-cache-ttl` | `30s` | 应用台账缓存的保底刷新间隔（`APPS_CHANGED` 广播会立即重载） |
| `nexus.ws.cluster.apps-import-on-startup` | `false` | 首次从单机迁到集群时导入本地 `push-apps.json`（只开一次） |
| `spring.session.timeout` / `server.servlet.session.timeout` | `30m` / `30m` | 集群会话失效时间 / 单机会话失效时间 |
| `spring.data.redis.*` | 无 | 集群模式的 Redis 连接（host/port 或 cluster.nodes 二选一） |

---

## Docker 部署

`nexus-websocket/` 目录下已提供 `Dockerfile` 与 `docker-compose.yml`：

```bash
cd nexus-websocket

# 方式一：编排（推荐）
docker compose up -d --build

# 方式二：先打镜像再跑
./scripts/docker-build.sh       # Windows: scripts\docker-build.cmd
docker run -d --name nexus-websocket \
  -p 9090:9090 -p 8089:8089 -p 9091:9091 \
  simonking/nexus-websocket:latest

docker compose logs -f
docker compose down
```

> 三个端口都需要暴露：9090（WebSocket）、8089（HTTP / 管理页 / REST 推送）、9091（TCP 推送通道）。

集群模式不用改镜像，用环境变量打开（entrypoint 会先校验开关与 Redis 地址）：

```bash
# Redis Cluster（Redis 3.x 必须传 resp2）
NEXUS_WS_CLUSTER_ENABLED=true \
REDIS_CLUSTER_NODES=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
NODE_ID=ws-node-1 NODE_SEQ=1 REDIS_PROTOCOL=resp2 \
docker compose up -d

# 单节点 Redis
NEXUS_WS_CLUSTER_ENABLED=true REDIS_HOST=127.0.0.1 REDIS_PORT=6379 \
NODE_ID=ws-node-1 NODE_SEQ=1 docker compose up -d
```

> 多副本部署时每个副本的 `SERVER_PORT` / `NEXUS_WS_WS_PORT` / `NEXUS_WS_TCP_PORT` 必须错开，并显式传 `NODE_ID` / `NODE_SEQ`。

---

## 已知限制

| 限制 | 影响 | 演进方向 |
| --- | --- | --- |
| 集群依赖 Redis | 多节点部署需要可用的 Redis（3.x 需 RESP2）；单机不依赖 | 引入其它总线实现 |
| 连接表仍是每节点内存 | 集群只做广播与聚合，不做连接迁移 | 连接表外置 + 会话粘滞 |
| 无消息历史 | 断线期间消息丢失，需重连后拉全量 | 引入 Redis 后补发 |
| TCP 通道无应用鉴权 | 只依赖内网端口隔离 | 按业务需求追加令牌或 mTLS |
| ApiKey 明文 | 请求可重放 | HMAC 签名 + nonce |
| 同步扇出 | 慢消费者阻塞推送线程 | 每连接有界出站队列 |
| 运维接口暴露明文 apiKey | 需限制内网访问 | 对接统一登录 / 审计 |

明确非目标：消息必达、离线补推、跨实例路由、消息持久化、端到端加密。
