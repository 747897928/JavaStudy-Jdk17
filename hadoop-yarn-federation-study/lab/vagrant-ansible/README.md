# Vagrant + Ansible Lab

这套实验环境现在分两段：

1. `site.yml`
   负责起基础环境、安装软件、生成和分发 keytab、铺配置。
2. `bringup.yml`
   负责自动启动 `KDC/ZooKeeper/HDFS/JHS/RM/NM/Router`、导入联邦权重并跑冒烟。

这样做的好处是：

1. 你可以只重跑环境铺设
2. 也可以只重跑服务启动和冒烟
3. 出错时更容易判断问题在“机器准备”还是“集群 bring-up”

## 为什么选 Vagrant + Ansible

相对 Docker，这套组合更接近你后面真正会碰到的 Linux 主机问题：

1. 主机名
2. `/etc/hosts`
3. Unix 用户和文件权限
4. `keytab` 分发
5. `container-executor` 权限

## 前置要求

### Windows 主机推荐组合

1. `VirtualBox` 或 `VMware Workstation`
2. `Vagrant`
3. `WSL2 + Ubuntu`
4. `ansible-core`

如果你直接在 Linux 主机上做，这套东西会更顺手。

## 虚机拓扑

1. `kdc1` -> `192.168.56.11`
2. `nn1` -> `192.168.56.12`
3. `prod1` -> `192.168.56.13`
4. `dr1` -> `192.168.56.14`
5. `router1` -> `192.168.56.15`

## 资源建议

默认资源：

1. `kdc1`: 2GB
2. `nn1/prod1/dr1`: 4GB
3. `router1`: 3GB

如果你机器资源紧张，可以先把 `LAB_MEMORY` 调低，但不要低得太夸张。

## 快速开始

### 1. 起虚机

```bash
cd hadoop-yarn-federation-study/lab/vagrant-ansible
vagrant up
```

如果 `generic/rocky9` box 拉不下来，可以切换：

```bash
LAB_BOX=bento/rockylinux-9 vagrant up
```

### 2. 跑环境铺设

```bash
cd hadoop-yarn-federation-study/lab/vagrant-ansible/ansible
ansible-playbook site.yml
```

### 3. 自动 bring-up

```bash
cd hadoop-yarn-federation-study/lab/vagrant-ansible/ansible
ansible-playbook bringup.yml
```

### 4. 如果只想重跑某一段

环境铺设：

```bash
ansible-playbook site.yml
```

服务启动：

```bash
ansible-playbook bringup.yml --tags bringup
```

只跑冒烟：

```bash
ansible-playbook bringup.yml --tags smoke
```

## 可以单独跑的 tag

```bash
ansible-playbook site.yml --tags base
ansible-playbook site.yml --tags kdc
ansible-playbook site.yml --tags keytabs
ansible-playbook site.yml --tags zookeeper
ansible-playbook site.yml --tags configs
ansible-playbook bringup.yml --tags hdfs
ansible-playbook bringup.yml --tags yarn
ansible-playbook bringup.yml --tags router
ansible-playbook bringup.yml --tags smoke
```

## 这套 lab 当前自动化到哪一步

### 已自动化

1. 虚机定义
2. 基础包安装
3. Kerberos 客户端配置
4. KDC 初始化
5. principal/keytab 生成
6. keytab 抓取和分发
7. Hadoop 和 ZooKeeper 安装
8. 联邦配置分发
9. NameNode 首次格式化
10. ZooKeeper / HDFS / JHS / RM / NM / Router 启动
11. Router 联邦权重导入
12. 基于 Router 的 YARN 冒烟

### 仍建议你理解并能手动做

1. `hdfs namenode -format` 的意义
2. HDFS 初始目录为什么要这些权限
3. 每个 daemon 用哪个 principal/keytab 启动
4. `routeradmin -policy` 实际在做什么
5. 冒烟失败时应该去哪里看日志

## 故障时先看哪里

1. [04-troubleshooting.md](../../docs/04-troubleshooting.md)
2. `nn1/prod1/dr1/router1` 上的 daemon 日志
3. `klist` 和 `kinit -kt` 校验
4. `jps` 看 JVM 进程是否起来
5. `yarn application -status` 是否能通过 Router 返回结果

## 当前边界

这套 lab 先服务于“学习联邦 + Kerberos + 统一提交/统一查询”：

1. 子集群内部先不做 RM HA
2. Router 先只放一个
3. 联邦 StateStore 先用单节点 ZooKeeper

如果你要更接近生产，后面再接 [08-production-grade-roadmap.md](../../docs/08-production-grade-roadmap.md)。
