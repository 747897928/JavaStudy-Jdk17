# 02. CentOS 风格 Kerberos 从 0 到 1

这一篇只解决一件事：先把 `Kerberos + principal + keytab + 基础校验` 跑通，为后面的 Hadoop/YARN 联邦做准备。

## 1. 约定的实验环境

### 1.1 机器角色

1. `kdc1.lab.example.com`: KDC
2. `nn1.lab.example.com`: NameNode + ZooKeeper + JobHistory
3. `prod1.lab.example.com`: `SC-PROD`
4. `dr1.lab.example.com`: `SC-DR`
5. `router1.lab.example.com`: Router + Client

### 1.2 统一约定

1. Kerberos Realm: `EXAMPLE.COM`
2. 域名后缀: `lab.example.com`
3. Hadoop 安装目录: `/opt/hadoop-3.4.3`
4. keytab 目录: `/etc/security/keytabs`
5. 服务用户:
   1. `hdfs:hadoop`
   2. `yarn:hadoop`
   3. `mapred:hadoop`

### 1.3 Java 版本建议

实验环境建议先用 `OpenJDK 11`，不要一上来用 JDK 17。原因很简单：

1. Hadoop 3.4.x 官方资料明确延续了 Java 11 的运行支持路径。
2. 学习期先用更稳的组合，能减少“到底是 Java 版本还是 Kerberos 配置错了”的排障干扰。

## 2. 所有节点先做的基础准备

### 2.1 配置主机名和 hosts

所有节点都要能做正反向解析，至少实验环境里要保证 `/etc/hosts` 一致：

```text
192.168.56.11 kdc1.lab.example.com kdc1
192.168.56.12 nn1.lab.example.com nn1
192.168.56.13 prod1.lab.example.com prod1
192.168.56.14 dr1.lab.example.com dr1
192.168.56.15 router1.lab.example.com router1
```

这是 Kerberos 成功率的第一前提。Hadoop Secure Mode 官方文档明确提到，forward/reverse lookup 必须正确。

### 2.2 时间同步

Kerberos 对时钟偏差很敏感。先启用 `chronyd`：

```bash
sudo yum install -y chrony
sudo systemctl enable --now chronyd
chronyc tracking
```

如果时间不同步，后面最常见的报错就是：

```text
Clock skew too great
```

### 2.3 安装 JDK 和基础工具

```bash
sudo yum install -y java-11-openjdk-devel krb5-server krb5-libs krb5-workstation
java -version
```

## 3. 创建 Hadoop 系统用户

在所有 Hadoop 节点执行：

```bash
sudo groupadd -f hadoop
id hdfs >/dev/null 2>&1 || sudo useradd -g hadoop hdfs
id yarn >/dev/null 2>&1 || sudo useradd -g hadoop yarn
id mapred >/dev/null 2>&1 || sudo useradd -g hadoop mapred
```

为什么这样做：

1. Hadoop Secure Mode 官方文档建议 `hdfs`、`yarn`、`mapred` 用不同 Unix 用户。
2. 同时建议它们共享一个组，例如 `hadoop`。

## 4. KDC 节点配置

以下步骤主要在 `kdc1` 上执行。

### 4.1 准备配置文件

把本项目里的模板复制到标准路径：

1. [krb5.conf](../conf-templates/krb5.conf)
2. [kdc.conf](../conf-templates/kdc.conf)
3. [kadm5.acl](../conf-templates/kadm5.acl)

CentOS 常见路径：

```bash
sudo cp conf-templates/krb5.conf /etc/krb5.conf
sudo cp conf-templates/kdc.conf /var/kerberos/krb5kdc/kdc.conf
sudo cp conf-templates/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl
sudo chmod 600 /var/kerberos/krb5kdc/kadm5.acl
```

### 4.2 初始化 KDC 数据库

```bash
sudo kdb5_util create -s -r EXAMPLE.COM
```

MIT Kerberos 官方安装文档推荐用 `kdb5_util create -r REALM -s` 创建数据库和 stash file。

### 4.3 创建管理员 principal

```bash
sudo kadmin.local -q "addprinc admin/admin@EXAMPLE.COM"
```

### 4.4 启动 KDC 服务

```bash
sudo systemctl enable --now krb5kdc
sudo systemctl enable --now kadmin
sudo systemctl status krb5kdc --no-pager
sudo systemctl status kadmin --no-pager
```

### 4.5 验证管理员登录

```bash
kinit admin/admin@EXAMPLE.COM
klist
```

## 5. 生成 Hadoop 服务 principal 和 keytab

本项目里已经给了一个脚本：

[create-service-principals.sh](../scripts/create-service-principals.sh)

它会创建下面这些 principal：

1. `nn/_HOST@EXAMPLE.COM`
2. `dn/_HOST@EXAMPLE.COM`
3. `rm/_HOST@EXAMPLE.COM`
4. `nm/_HOST@EXAMPLE.COM`
5. `router/_HOST@EXAMPLE.COM`
6. `jhs/_HOST@EXAMPLE.COM`
7. `HTTP/_HOST@EXAMPLE.COM`
8. 可选的测试用户 principal

### 5.1 在 KDC 上执行脚本

```bash
chmod +x scripts/create-service-principals.sh
sudo REALM=EXAMPLE.COM KEYTAB_DIR=/tmp/hadoop-keytabs ./scripts/create-service-principals.sh
```

### 5.2 把 keytab 分发到目标节点

分发完成后，在目标节点上执行：

```bash
sudo mkdir -p /etc/security/keytabs
sudo cp /tmp/nn.service.keytab /etc/security/keytabs/nn.service.keytab
sudo chown hdfs:hadoop /etc/security/keytabs/nn.service.keytab
sudo chmod 400 /etc/security/keytabs/nn.service.keytab
```

其它服务同理：

1. 把 `rm-prod.service.keytab` 下发到 `prod1` 后，重命名为 `/etc/security/keytabs/rm.service.keytab`
2. 把 `rm-dr.service.keytab` 下发到 `dr1` 后，重命名为 `/etc/security/keytabs/rm.service.keytab`
3. 把 `nm-prod.service.keytab` 下发到 `prod1` 后，重命名为 `/etc/security/keytabs/nm.service.keytab`
4. 把 `nm-dr.service.keytab` 下发到 `dr1` 后，重命名为 `/etc/security/keytabs/nm.service.keytab`
5. `router.service.keytab` 归 `yarn:hadoop`
6. `jhs.service.keytab` 归 `mapred:hadoop`
7. `dn-prod.service.keytab`、`dn-dr.service.keytab` 需要分别下发到对应 DN 节点，并统一重命名为 `/etc/security/keytabs/dn.service.keytab`
8. `http.service.keytab` 可以按运行该 Web 进程的用户归属

## 6. 每个节点的本地验证

### 6.1 服务 principal 验证

在 `prod1` 上验证 RM/NM：

```bash
sudo -u yarn kinit -kt /etc/security/keytabs/rm.service.keytab rm/prod1.lab.example.com@EXAMPLE.COM
sudo -u yarn klist
sudo -u yarn kdestroy

sudo -u yarn kinit -kt /etc/security/keytabs/nm.service.keytab nm/prod1.lab.example.com@EXAMPLE.COM
sudo -u yarn klist
sudo -u yarn kdestroy
```

在 `nn1` 上验证 NN/JHS：

```bash
sudo -u hdfs kinit -kt /etc/security/keytabs/nn.service.keytab nn/nn1.lab.example.com@EXAMPLE.COM
sudo -u hdfs klist
sudo -u hdfs kdestroy

sudo -u mapred kinit -kt /etc/security/keytabs/jhs.service.keytab jhs/nn1.lab.example.com@EXAMPLE.COM
sudo -u mapred klist
sudo -u mapred kdestroy
```

### 6.2 HTTP principal 验证

```bash
sudo klist -kte /etc/security/keytabs/http.service.keytab
```

如果后面 RM/NM/JHS/Router Web UI 要走 SPNEGO，这个 keytab 非常关键。

## 7. Hadoop 需要的 auth_to_local

Hadoop Secure Mode 官方文档给了一个非常重要的示例，因为 Hadoop daemon 通常是以下 Unix 用户启动：

1. `hdfs`
2. `yarn`
3. `mapred`

但 principal 名一般是：

1. `nn/_HOST`
2. `dn/_HOST`
3. `rm/_HOST`
4. `nm/_HOST`
5. `jhs/_HOST`

所以必须在 `core-site.xml` 里配置 `hadoop.security.auth_to_local`，把它们映射到本地服务账户。这个项目的 [core-site.xml](../conf-templates/core-site.xml) 已经带了一份可用样例，并额外给 `router/_HOST` 映射到了 `yarn`。

## 8. 这里最容易踩的坑

### 8.1 principal 用了短主机名

错例：

```text
rm/prod1@EXAMPLE.COM
```

建议：

```text
rm/prod1.lab.example.com@EXAMPLE.COM
```

因为 Hadoop 官方文档和 `_HOST` 展开逻辑默认就是按 FQDN 工作。

### 8.2 改了 principal 之后没重发 keytab

很常见：

1. 先生成过 keytab
2. 后来又删 principal、重建 principal
3. 结果目标节点还拿着旧 keytab

这时就会出现：

```text
Preauthentication failed
Keytab contains no suitable keys
```

### 8.3 时间不同步

Kerberos 世界里，时间偏差是第一大嫌疑人。

### 8.4 DNS 和 /etc/hosts 不一致

这会引出一串很绕的错误：

1. `Server not found in Kerberos database`
2. `No valid credentials provided`
3. `GSS initiate failed`

## 9. 下一步

Kerberos 跑通后，再进入下一篇：

[03-hadoop-3.4.3-federation-lab.md](./03-hadoop-3.4.3-federation-lab.md)

## 参考资料

1. MIT Kerberos Installing KDCs: https://web.mit.edu/Kerberos/krb5-latest/doc/admin/install_kdc.html
2. Apache Hadoop Secure Mode 3.4.1: https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SecureMode.html
