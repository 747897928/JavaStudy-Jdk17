# 06. Linux 命令入门与脚本拆解

这篇是给 Linux 新手看的。

目标不是把 Shell 讲全，而是让你能看懂这个项目里的三个脚本：

1. [prepare-host-dirs.sh](../scripts/prepare-host-dirs.sh)
2. [create-service-principals.sh](../scripts/create-service-principals.sh)
3. [smoke-test-federation.sh](../scripts/smoke-test-federation.sh)

## 1. 先认识脚本第一行

```bash
#!/usr/bin/env bash
```

这叫 `shebang`，意思是：

“请用 `bash` 来执行这个脚本。”

## 2. 这一行特别常见

```bash
set -euo pipefail
```

你可以先简单理解成“严格模式”：

1. `-e`: 只要有命令失败，脚本就退出
2. `-u`: 用了没定义的变量，脚本就退出
3. `pipefail`: 管道里任意一步失败，都算失败

这能减少脚本悄悄执行错还继续跑下去的情况。

## 3. 环境变量怎么读

脚本里经常看到：

```bash
REALM="${REALM:-EXAMPLE.COM}"
```

意思是：

1. 如果外部已经传了 `REALM`
2. 就用外部值
3. 否则默认用 `EXAMPLE.COM`

例如：

```bash
REALM=TEST.COM ./create-service-principals.sh
```

## 4. 你最常看到的基础命令

### 4.1 `mkdir -p`

创建目录。

```bash
mkdir -p /data/hadoop/yarn/log
```

`-p` 的意思是：

1. 父目录不存在也一起创建
2. 目录已存在也不报错

### 4.2 `rm -f`

删除文件。

```bash
rm -f /tmp/*.keytab
```

`-f` 的意思是强制删除，不反复确认。

### 4.3 `groupadd -f`

创建用户组。

```bash
groupadd -f hadoop
```

`-f` 表示组已经存在时也不要报错。

### 4.4 `useradd`

创建 Linux 用户。

```bash
useradd -g hadoop yarn
```

意思是创建用户 `yarn`，主组设为 `hadoop`。

### 4.5 `id`

查看用户是否存在、UID/GID 是多少。

```bash
id yarn
```

### 4.6 `chown`

修改文件属主和属组。

```bash
chown -R yarn:hadoop /data/hadoop/yarn
```

意思是把目录及其内容都改成：

1. 属主 `yarn`
2. 属组 `hadoop`

### 4.7 `chmod`

修改权限。

```bash
chmod 400 /etc/security/keytabs/rm.service.keytab
chmod 755 /data/hadoop/yarn/log
```

常见数字先记住这几个：

1. `400`: 只有拥有者可读
2. `700`: 只有拥有者可读可写可执行
3. `755`: 拥有者可写，其他人只读和执行
4. `6050`: Hadoop `container-executor` 常见权限

## 5. 逻辑判断怎么读

脚本里常见这种写法：

```bash
id yarn >/dev/null 2>&1 || useradd -g hadoop yarn
```

你可以把它翻译成：

1. 先执行 `id yarn`
2. 如果成功，后面的不执行
3. 如果失败，执行 `useradd -g hadoop yarn`

其中：

1. `>/dev/null` 表示丢掉标准输出
2. `2>&1` 表示把错误输出也并到同一个地方

所以整句就是：

“如果 `yarn` 用户不存在，就创建它。”

## 6. 函数怎么读

在 `create-service-principals.sh` 里有：

```bash
add_randkey() {
    local principal="$1"
    ...
}
```

意思是定义了一个函数 `add_randkey`。

里面：

1. `local principal="$1"` 表示取函数的第一个参数
2. `local` 表示这是函数内部变量

调用时像这样：

```bash
add_randkey "rm/prod1.lab.example.com@EXAMPLE.COM"
```

## 7. Kerberos 相关命令怎么理解

### 7.1 `kadmin.local -q`

在 KDC 本机直接执行 Kerberos 管理命令。

```bash
kadmin.local -q "getprinc rm/prod1.lab.example.com@EXAMPLE.COM"
```

`-q` 表示后面跟一条命令字符串。

### 7.2 `kinit -kt`

用 keytab 登录。

```bash
kinit -kt /etc/security/keytabs/rm.service.keytab rm/prod1.lab.example.com@EXAMPLE.COM
```

你可以理解成：

1. `-k`: 用 keytab
2. `-t`: 指定 keytab 文件

### 7.3 `klist`

查看当前票据或 keytab 内容。

```bash
klist
klist -kte /etc/security/keytabs/http.service.keytab
```

## 8. 文本处理命令怎么读

### 8.1 `grep -o`

只输出匹配到的那一部分。

```bash
grep -o 'application_[0-9_]*'
```

这里的作用是从一大段日志里提取：

```text
application_1234567890000_0001
```

### 8.2 `tail -n 1`

取最后一行。

```bash
tail -n 1
```

### 8.3 `tee /dev/stderr`

把输出一边继续往后传，一边再打印出来。

这个在调试脚本时很有用。

## 9. 条件判断怎么读

在 `smoke-test-federation.sh` 里有：

```bash
if [[ ! -f "${EXAMPLE_JAR}" ]]; then
    echo "Example jar not found: ${EXAMPLE_JAR}" >&2
    exit 1
fi
```

意思是：

1. 如果文件不存在
2. 打印错误信息
3. 退出脚本

其中：

1. `[[ ... ]]` 是 Bash 条件判断语法
2. `!` 表示取反
3. `-f` 表示“这是一个普通文件”
4. `>&2` 表示把内容输出到错误流

## 10. `exit 1` 是什么意思

Shell 里通常约定：

1. `exit 0`: 成功
2. `exit 1`: 失败

所以脚本里写 `exit 1`，就是在明确告诉调用方“这次执行失败了”。

## 11. 你现在最该掌握的最小命令集

如果你是 Linux 新手，先只练这些：

1. `pwd`
2. `ls -l`
3. `cd`
4. `mkdir -p`
5. `cp`
6. `mv`
7. `rm -f`
8. `chmod`
9. `chown`
10. `cat`
11. `grep`
12. `tail`
13. `kinit`
14. `klist`
15. `kdestroy`

## 12. 建议你的练习方法

### 第一步

自己手敲这些命令：

```bash
mkdir -p /tmp/demo/a
touch /tmp/demo/a/test.txt
ls -l /tmp/demo/a
chmod 400 /tmp/demo/a/test.txt
ls -l /tmp/demo/a
```

### 第二步

再练环境变量：

```bash
NAME=wizard
echo "$NAME"
```

### 第三步

最后再去读本项目脚本，会轻松很多。

## 13. 对应到本项目脚本，建议这样读

1. 先看 [prepare-host-dirs.sh](../scripts/prepare-host-dirs.sh)
   这个脚本最简单，主要是目录、用户、权限。
2. 再看 [create-service-principals.sh](../scripts/create-service-principals.sh)
   这里开始涉及函数、变量和 Kerberos 命令。
3. 最后看 [smoke-test-federation.sh](../scripts/smoke-test-federation.sh)
   这里会串起 `kinit`、Hadoop 命令和文本提取。
