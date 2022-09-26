jobpool
===
jobpool 是一款基于RAFT协议的调度引擎。

Quick Start
---
#### How to use

```shell script
# 下载依赖包
go mod download
# 清理依赖
go mod tidy
# 将依赖复制到vendor下
go mod vendor
# 编译
go build
# 开发模式运行
./jobpool agent --dev

```

### 项目目录

本项目是基于nomad源码的重写，目的是增强调度器，轻量化执行器。

<details>
<summary>目录结构</summary>
<pre>
<code>
├── client 客户端
│   ├── config/ 配置
│   ├── structs/ 业务实体
│   ├── client 客户端逻辑
│   ├── servers/ 服务端管理器
│   ├── stats/ 状态逻辑
│   ├── heartbeat 心跳维持逻辑
│   ├── state/ 客户端持久化（不需要一致性存储的部分）
├── command 命令行（系统入口）
│   ├── agent/ 代理
│   │   ├── http Rest服务端
│   │   ├── http_router 路由（新服务地址需要在此注册）
│   │   ├── XXX_endpoint 接口层（处理参数和返回）
│   ├── conf/ 配置
│   ├── client/ Rest客户端（构建请求）
│   ├── constant/ 命令行相关常量
│   ├── model/ http查询参数相关实体
│   ├── commands 命令行注册工厂
│   ├── XXX_command 业务模块命令行
├── core 核心部件及服务端
│   ├── fsm 有限状态机（用于raftApply）
│   ├── server 服务端逻辑
│   ├── constant/ 系统常量
│   ├── dto/ 请求响应实体
│   ├── XXX_service 各模块业务逻辑层
│   ├── state/
│   │   ├── XXX_store 各模块持久化层
│   │   ├── paginator 分页组件
│   │   ├── schema 表信息（新表需要在此注册）
│   ├── structs/ 业务实体
│   ├── scheduler/ 周期调度相关
├── helper 帮助类
│   ├── boltdd
│   ├── codec
│   ├── log
</code>
</pre>
</details>
