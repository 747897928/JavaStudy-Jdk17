# PostgreSQL 本地安装与主从演示

这份文档默认你在 Windows 上开发，而且本机还没装 PostgreSQL。

## 1. 推荐方案

学习这个 Demo 时，优先推荐这两种方式：

- 最省事：Docker Desktop + 单机 PostgreSQL 容器
- 想学主从：Docker Desktop + `docker-compose.postgres-ha.yml`

原因很直接：

- 不污染本机环境
- 删容器就能重来
- 主从拓扑比原生安装更容易复现

## 2. 先装 Docker Desktop

Windows 上直接安装 Docker Desktop 即可。装好后先检查：

```powershell
docker --version
docker compose version
```

如果这两条命令都能跑通，再继续下面的步骤。

## 3. 单机 PostgreSQL 启动

### 启动

```powershell
docker run --name r2dbc-demo-pg `
  -e POSTGRES_USER=demo `
  -e POSTGRES_PASSWORD=demo `
  -e POSTGRES_DB=reactive_order_demo `
  -p 5432:5432 `
  -d postgres:17
```

### 检查

```powershell
docker ps
docker logs r2dbc-demo-pg
```

### 连接信息

- Host: `localhost`
- Port: `5432`
- Database: `reactive_order_demo`
- Username: `demo`
- Password: `demo`

你可以用 DBeaver / DataGrip 直接连，也可以进容器执行：

```powershell
docker exec -it r2dbc-demo-pg psql -U demo -d reactive_order_demo
```

## 4. 启动主从拓扑

在仓库根目录执行：

```powershell
docker compose -f spring-data-r2dbc-demo/docker-compose.postgres-ha.yml up -d
```

### 检查容器状态

```powershell
docker ps
docker logs spring-data-r2dbc-demo-pg-0
docker logs spring-data-r2dbc-demo-pg-1
```

### 查看主从角色

先看 `pg-0`：

```powershell
docker exec -it spring-data-r2dbc-demo-pg-0 psql -U demo -d reactive_order_demo -c "select inet_server_addr(), inet_server_port(), pg_is_in_recovery();"
```

再看 `pg-1`：

```powershell
docker exec -it spring-data-r2dbc-demo-pg-1 psql -U demo -d reactive_order_demo -c "select inet_server_addr(), inet_server_port(), pg_is_in_recovery();"
```

通常你会看到：

- 一个节点 `pg_is_in_recovery = f`，它是 primary
- 另一个节点 `pg_is_in_recovery = t`，它是 standby

## 5. 让应用连接主从拓扑

启动 PostgreSQL 主从之后，用 `ha` profile 启动应用：

```powershell
mvn -pl spring-data-r2dbc-demo spring-boot:run "-Dspring-boot.run.profiles=ha"
```

然后请求：

```powershell
curl http://localhost:8083/api/topology
```

返回里你重点看两个字段：

- `writer.inRecovery`
- `reader.inRecovery`

理想情况下：

- `writer.inRecovery = false`
- `reader.inRecovery = true`

## 6. 如何做一次故障转移观察

> 这部分的目标是“观察应用连接行为”，不是生产级运维手册。

### Step 1

先确认应用已经用 `ha` profile 启动，并且 `/api/topology` 可访问。

### Step 2

停止当前 primary。假设当前 primary 是 `pg-0`：

```powershell
docker stop spring-data-r2dbc-demo-pg-0
```

### Step 3

观察 `pg-1` 日志，等它提升为新的 primary：

```powershell
docker logs spring-data-r2dbc-demo-pg-1
```

### Step 4

再次调用：

```powershell
curl http://localhost:8083/api/topology
```

这时你应该重点观察：

- writer 是否重新连到了新的 primary
- reader 是否仍然保持可读

## 7. 常见问题

### 端口被占用

如果 `5432` 或 `5433` 已经被本机别的 PostgreSQL 占用：

- 改 compose 里的宿主机端口
- 同时改 `application.yml` 里的连接串

### 容器删了数据没了

这是因为卷被删了。重新跑学习 Demo 没问题，但如果你想保留数据，不要随便执行：

```powershell
docker compose -f spring-data-r2dbc-demo/docker-compose.postgres-ha.yml down -v
```

### 读接口偶尔查不到刚写入的数据

这通常不是 bug，而是复制延迟导致的最终一致表现。这个 Demo 正好用来观察这个事实。

## 8. 什么时候考虑原生安装 PostgreSQL

如果你下一步要深入学这些，再考虑原生安装：

- WAL、复制槽、归档
- `postgresql.conf` / `pg_hba.conf`
- 流复制与物理备库
- Patroni、repmgr、PgBouncer、HAProxy

但对当前这个学习项目来说，Docker 路线已经足够高效。
