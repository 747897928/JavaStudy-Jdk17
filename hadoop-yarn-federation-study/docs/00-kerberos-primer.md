# 00. Kerberos 入门速读

这篇不是 Hadoop 配置文档，而是给完全没学过 Kerberos 的人打基础。

目标只有两个：

1. 先把名词听懂
2. 再把它和 Hadoop/YARN 对上

## 1. Kerberos 到底是干什么的

一句话：

Kerberos 是一个“基于票据的身份认证系统”。

你可以把它理解成：

1. 先向认证中心证明“我是谁”
2. 认证中心给你一张临时通行证
3. 之后你拿这张通行证去访问具体服务

这样做的好处是：

1. 不用每次都把密码直接发给服务端
2. 服务之间可以相互信任同一个认证中心
3. 比较适合 Hadoop 这种多进程、多节点的分布式系统

## 2. 先认识最重要的 8 个词

### 2.1 Realm

Realm 就是 Kerberos 的认证域，通常是大写，比如：

```text
EXAMPLE.COM
```

它有点像“安全边界 + 命名空间”。

### 2.2 KDC

`KDC` 是 `Key Distribution Center`，可以理解成 Kerberos 的认证中心。

它通常包含两部分能力：

1. 验证你是不是你
2. 给你发票据

### 2.3 principal

principal 就是 Kerberos 世界里的身份名。

常见形式：

```text
alice@EXAMPLE.COM
rm/prod1.lab.example.com@EXAMPLE.COM
HTTP/router1.lab.example.com@EXAMPLE.COM
```

可以简单理解为：

1. 用户身份：`alice@EXAMPLE.COM`
2. 服务身份：`service/host@REALM`

### 2.4 keytab

keytab 是“服务账户的免交互登录凭据文件”。

服务进程没法像人一样手动输密码，所以通常靠 keytab 登录。

比如：

```text
/etc/security/keytabs/rm.service.keytab
```

### 2.5 TGT

`TGT` 是 `Ticket Granting Ticket`，可以理解成“总通行证”。

你执行 `kinit` 成功后，通常先拿到的就是 TGT。

### 2.6 Service Ticket

当你拿着 TGT 去访问某个具体服务时，KDC 会再给你一张“访问这个服务的票”。

这就是 service ticket。

### 2.7 SPNEGO

SPNEGO 常出现在 Web UI 场景里，比如 Hadoop 的：

1. NameNode UI
2. ResourceManager UI
3. JobHistory UI
4. Router UI

你看到 `HTTP/_HOST@REALM` 这种 principal，基本就和 Web/SPNEGO 有关。

### 2.8 `_HOST`

Hadoop 配置里经常写：

```text
rm/_HOST@EXAMPLE.COM
HTTP/_HOST@EXAMPLE.COM
```

意思不是 principal 真的叫 `_HOST`，而是运行时会替换成当前机器的主机名，一般是 FQDN。

例如：

```text
rm/prod1.lab.example.com@EXAMPLE.COM
```

## 3. Kerberos 是怎么工作的

先看不带 Hadoop 的简化流程：

1. 用户执行 `kinit alice@EXAMPLE.COM`
2. KDC 验证身份后发一个 TGT
3. 用户去访问 `rm/prod1.lab.example.com@EXAMPLE.COM`
4. Kerberos 再给用户发一个访问 RM 的 service ticket
5. RM 验证票据后，确认“这个请求确实来自 alice”

你不需要一开始就背协议细节，先记住这个模式：

`先拿总票，再拿服务票`

## 4. 放到 Hadoop 里怎么理解

Hadoop 开启 Kerberos 后，几乎每个核心角色都有自己的 principal 和 keytab。

比如：

1. NameNode: `nn/_HOST@REALM`
2. DataNode: `dn/_HOST@REALM`
3. ResourceManager: `rm/_HOST@REALM`
4. NodeManager: `nm/_HOST@REALM`
5. JobHistory: `jhs/_HOST@REALM`
6. Router: `router/_HOST@REALM`
7. Web UI: `HTTP/_HOST@REALM`

这意味着：

1. 进程启动前要能用 keytab 成功 `kinit`
2. 进程之间通信时会带 Kerberos 身份
3. Web UI 访问时也可能要走 SPNEGO

## 5. 为什么 Hadoop 里还会出现 Unix 用户

这是新手最容易混淆的一点。

Kerberos principal 和 Linux 用户不是同一个东西。

例如：

1. Kerberos principal: `rm/prod1.lab.example.com@EXAMPLE.COM`
2. Linux 用户: `yarn`

Hadoop 需要一条映射规则把两者对应起来，这就是：

```text
hadoop.security.auth_to_local
```

意思是：

1. Kerberos 负责认证你是谁
2. Hadoop 再把这个身份映射到本地 Unix 用户

## 6. 你在这个项目里最常见的 principal

### 6.1 服务 principal

```text
nn/nn1.lab.example.com@EXAMPLE.COM
dn/prod1.lab.example.com@EXAMPLE.COM
rm/prod1.lab.example.com@EXAMPLE.COM
nm/prod1.lab.example.com@EXAMPLE.COM
router/router1.lab.example.com@EXAMPLE.COM
jhs/nn1.lab.example.com@EXAMPLE.COM
```

### 6.2 Web principal

```text
HTTP/nn1.lab.example.com@EXAMPLE.COM
HTTP/prod1.lab.example.com@EXAMPLE.COM
HTTP/dr1.lab.example.com@EXAMPLE.COM
HTTP/router1.lab.example.com@EXAMPLE.COM
```

### 6.3 测试用户 principal

```text
sparkuser@EXAMPLE.COM
```

## 7. 你先学会这几个命令就够了

### 7.1 `kinit`

登录，拿票据。

```bash
kinit sparkuser@EXAMPLE.COM
kinit -kt /etc/security/keytabs/rm.service.keytab rm/prod1.lab.example.com@EXAMPLE.COM
```

### 7.2 `klist`

看当前票据。

```bash
klist
klist -kte /etc/security/keytabs/http.service.keytab
```

### 7.3 `kdestroy`

清掉当前票据缓存。

```bash
kdestroy
```

### 7.4 `kadmin.local`

在 KDC 本机管理 principal 和 keytab。

```bash
sudo kadmin.local -q "getprinc rm/prod1.lab.example.com@EXAMPLE.COM"
sudo kadmin.local -q "addprinc -randkey rm/prod1.lab.example.com@EXAMPLE.COM"
sudo kadmin.local -q "ktadd -k /tmp/rm.service.keytab rm/prod1.lab.example.com@EXAMPLE.COM"
```

## 8. 为什么总强调 FQDN

Kerberos 和 Hadoop 都非常依赖主机名一致性。

建议统一使用：

```text
prod1.lab.example.com
```

不要混用：

```text
prod1
```

否则你很容易遇到：

1. `Server not found in Kerberos database`
2. `_HOST` 展开后和你实际建的 principal 不一致
3. Web/SPNEGO 验证失败

## 9. 先别追协议细节，先抓住这 4 个实战点

1. principal 是身份名
2. keytab 是服务登录凭据文件
3. `kinit/klist/kdestroy` 是最基础三件套
4. Hadoop 里 principal 最后还得映射到 Linux 用户

## 10. 我建议你的学习顺序

1. 先读这篇，把名词对齐
2. 再读 [02-centos-kerberos-bootstrap.md](./02-centos-kerberos-bootstrap.md)
3. 然后自己在一台 Linux 上只练 3 件事：
   1. 建 principal
   2. 导出 keytab
   3. 用 `kinit -kt` 登录
4. 最后再把它带进 Hadoop/YARN

## 11. 推荐资料

1. MIT Kerberos 安装文档: https://web.mit.edu/Kerberos/krb5-latest/doc/admin/install_kdc.html
2. MIT Kerberos 管理文档首页: https://web.mit.edu/kerberos/krb5-latest/doc/admin/index.html
3. Apache Hadoop Secure Mode: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SecureMode.html
