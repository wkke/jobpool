# Raft是什么

  Raft 是一种为了管理复制日志的一致性算法。
  
详细说明：https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md


## Raft请求及响应机制
系统中只有一个Leader并且其他的节点全部都是Flower，
Flower都是被动的，他们不会发送任何请求，只是简单的响应来自Leader或者候选人的请求。
Leader处理所有的客户端请求（如果一个客户端和Flower连接，那么Flower会把请求重定向给Leader）。

    
    以KV存储为例，流程如下：
    1、客户端发起请求
        command/agent/XXX_endpoint中发起请求httpServer.agent.RPC("Kv.ListKvs", &args, &out)
    2、客户端找到任意server
        client找到对应的server列表中的第一个
    3、指定server地址发起RPC请求
        client.connPool.RPC(c.Region(), server.Addr, method, args, reply)
    4、server端对应的方法收到请求
        core/XXX_service中方法得到响应（前提是在server中的endpoint中已注册该服务）
    5、server端将请求转发给leader
        n.srv.forward("Kv.ListKvs", args, args, reply)
    6、本人就是leader则直接往下进行
    7、本人不是leader则转发给leader（已是第二次转发）
        获取Leader地址、转发给leader
        r.connPool.RPC(r.config.Region, server.Addr, method, args, reply)
    8、Leader端对应的方法收到请求（同4）
        core/XXX_service中的方法
    9、运行业务逻辑返回请求结果

查看服务端节点信息命令如下：./jobpool server members
```shell script
Name           Address       Port  Status  Leader  Raft Version  Build  Datacenter  Region
node-1.global  192.168.0.89  4648  alive   false   3             1.0.0  dc1         global
node-2.global  192.168.0.89  5648  alive   true    3             1.0.0  dc1         global
node-3.global  192.168.0.89  6648  alive   false   3             1.0.0  dc1         global
```

则所有请求均会最终发送给node-2.global节点，并在该节点生成日志，然后将日志复制给从节点

系统启动后会在配置的data_dir路径下生成如下文件夹信息

<details>
<summary>目录结构</summary>
<pre>
<code>
├── server 服务器端
│   ├── raft/ 持久化的raft信息
│   │   ├── raft.db 日志信息
│   │   ├── snapshots/ 快照文件夹
│   │   ├── version 版本号
│   │   ├── peers.info 说明文件
</code>
</pre>
</details>

系统启动时读取文件并进行Restore
