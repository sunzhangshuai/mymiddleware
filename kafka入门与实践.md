# Kafka特性

- **消息持久化**

  > 数据落盘

- **高吞吐量**

  > 顺序读写磁盘、副本同步时零拷贝技术、主题分区、消息压缩、批量发送。
  >
  > 支持每秒百万级别的消息

- **扩展性**

  > 用zookeeper管理集群，更容易进行水平扩展

- **多客户端支持**

  > 客户端支持多种语言，大数据框架

- **kafka streams**

- **安全机制**

  > 1. 通过ssl支持生产者，消费者与代理连接的身份验证。
  > 2. 代理与zookeeper连接的身份验证。
  > 3. 通信时数据加密。
  > 4. 客户端读写权限认证。
  > 5. kafka支持与外部其他认证服务的集成。

- **数据备份**

  > 每个主题可以指定副本数。

- **轻量级**

  > 代理无状态。

- **消息压缩**

  > Gzip、Snappy、LZ4
  >
  > 多条消息可以放在一起组成MessageSet，再把messageSet放到一条消息里。

# 应用场景

> 1. 消息系统
> 2. 应用监控
> 3. 网站用户行为追踪
> 4. 流处理
> 5. 持久性日志

# 消费者

> Consumer，负责从kafka集群拉取消息

# 消费组

> 1. 每一个消费者都属于一个特定的消费组【ConsumerGroup】。
> 2. 不指定消费组的，属于默认消费组，名称是 `test-consumer-group`。
> 3. 消费者有全局唯一id，不指定则自动生成一个，格式为 `${groupId}-${hostName}-${timeStamp}-${UUID前8位字符}`。
> 4. 同一个主题的一条消息只能被一个消费组的一个消费者消费，但不同消费组的消费者可以同时消费该消息。

# 生产者

> Producer，负责把消息写入kafka集群

# 主题

> 1. 一组消息。
> 2. 生产者将消息发送到特定主题。
> 3. 消费者订阅主题或主题的一些分区。

# 消息

> 1. kafka通信的基本单位。
>
> 2. 组成：由固定长度的消息头和可变长度的消息体组成。
> 3. 在java中每一条消息称为Record。
> 4. 消息删除策略
>    1. 基于消息已存储的时间长度。
>    2. 基于分区的大小。

# 分区【Partition】

> 1. 一个主题可以有1-多个分区。
> 2. 分区由一系列消息组成，消息有序，不可变。
> 3. 分区在物理上对应一个文件夹，命名规则：主题名称+"-"+编号【0-分区总数-1】。
> 4. 指定分区数
>    1. 启动时加载的配置文件中配置。
>    2. 创建主题时指定。
>    3. 创建主题后修改分区数。
> 5. 分区内消息有序，跨分区无序。

# 副本【Replica】

> 1. 1个分区可以有1-多个副本。
> 2. 分区的副本分布在集群的不同代理上。
> 3. 副本可以看做一个日志对象。

- **Leader副本**

  > 1. 负责处理客户端的读写请求。
  > 2. leader失效，通过选举算法从follower选出一个新leader。

- **Follower副本**

  > 从leader副本同步数据。

# 偏移量

> 分区下严格有序的逻辑值，不代表消息在磁盘中的物理位置。

# 日志段

> 1. 一个日志由多个日志段组成。
> 2. 日志段是日志对象分片的最小单位。
> 3. 一个日志段对应磁盘上一个具体日志文件和两个索引文件。
>    1. .log：保存消息实际数据
>    2. .index：表示消息偏移量
>    3. .timeIndex：消息时间戳索引文件

# 代理【Broker】

> 1. kafka集群的一个实例。
>
> 2. 一个服务器可以有多个代理。
> 3. 每个代理都有全局唯一标识id。
> 4. id是非负整数。

# ISR

> 1. 保存所有与leader副本保持消息同步的所有副本对应的代理节点id。
>
> 2. 代理失效，副本节点从ISR中移除。
> 3. 代理失效：人为关闭、物理故障、心跳检测过期、网络延迟、进程崩溃、同步落后太多。

# ZooKeeper

> 1. 保存元数据信息。
> 2. 元数据信息：代理节点信息、kafka集群信息、旧版消费者信息、消费偏移量信息、主题信息、分区状态信息、分区副本分配方案信息、动态配置信息。
> 3. 用来维护kafka集群，方便进行水平扩展和数据迁移。

# KafkaServer管理

1. **启动单个结点命令**

   ```sh
   Kafka-server-start -daemon /usr/local/etc/kafka/server.properties
   ```

2. **关闭单个结点命令**

   ```sh
   kafka-server-stop
   ```

## 常用日志查看

## 运行日志

``````shell
/usr/local/Cellar/kafka/2.6.0_1/libexec/logs/server.log
``````

# Zookeeper管理

1. **客户端登录命令**

   ```shell
   zkcli -server ip:port 
   ```

2. **查看kafka集群结点列表**

   ```shell
   ls /brokers/ids
   ```

3. **查看leader副本信息**

   ``````shell
   get /controller
   ``````

4. **查看主题分区列表**

   ```SHELL
   ls /brokers/topics/topicName/partitions
   ```

5. **查看主题副本分布信息**

   ```shell
   get /brokers/topics/topicName
   ```

6. **查看主题个性化配置**

   ```shell
   get /config/topics/topicName
   ```

7. **删除主题元数据**

   ```shell
   deleteall /brokers/topics/topicName
   deleteall /config/topics/topicName
   ```

8. **查看待删除的主题**

   ```shell
   get /admin/delete_topics
   ```

   

# 主题管理

## 创建主题

1. 代理设置  `auto.create.topics.enable=true`【**默认**】，生产者向没有创建的主题发消息时，自动创建拥有`${num.partitions}`个分区和`${default.replication.factor}`个副本的主题。

   ```shell
   kafka-console-producer --broker-list localhost:9092,localhost:9093,localhost:9094 --topic topicName
   ```

2. 通过指令创建。

   ```shell
   kafka-topics --create --zookeeper ip:port[,ip:port] --replication-factor num --partitions num --topic topicName
   ```

   1. zookeeper：必传参数。
   2. partitions：必传参数。
   3. replication-factor：必传参数，副本数不能超过节点数。
   4. 可以通过--config config1=value1 来覆盖默认配置。

## 删除主题

1. 手动删除：

   1. 删除各节点该主题分区文件夹；
   2. 登录zookeeper客户端，删除主题元数据。

2. 通过指令删除。

   ```shell
   kafka-topics --delete --zookeeper ip:port[,ip:port] --topic topicName
   ```

   1. `delete.topics.enable=ture`：彻底删除主题对应的文件目录和元数据。
   2. `delete.topics.enable=false`：主题文件夹和元数据标记删除。

## 查看主题

1. 查看所有主题

   ```shell
   kafka-topics --list --zookeeper ip:port
   ```

2. 查看某个特定主题信息

   ```shell
   kafka-topics --describe --zookeeper ip:port --topic topicName
   ```

   1. 不加`--topic`：展示所有主题的详细信息。

3. 查看正在同步的主题

   ```shell
   kafka-topics --describe --zookeeper ip:port --under-replicated-partitions 
   ```

   1. 命令查到的分区要重点监控。

4. 查看没有leader的分区

   ```shell
   kafka-topics --describe --zookeeper ip:port --unavailable-partitions
   ```

5. 查看主题覆盖的配置

   ```shell
   kafka-topics --describe --zookeeper ip:port --topics-with-overrides --topic topicName
   ```

## 修改主题

1. 修改主题级别配置

   ```shell
   # 新增&覆盖
   kafka-configs  --zookeeper ip:port --topic kafka-zs --alter --add-config  config1 = value1
   # 删除
   kafka-configs  --zookeeper ip:port --topic kafka-zs --alter --delete-config  config1
   ```

2. 增加分区

   ```shell
   kafka-topics  --zookeeper ip:port --topic kafka-zs --alter --partitions num
   ```

   1. kafka不支持减少分区的操作。

# 生产者基本操作

## 启动生产者

```shell
kafka-console-producer --broker-list localhost:9092,localhost:9093,localhost:9094 --topic topicName
```

1. broker-list：必传参数，指定代理地址列表。
2. topic：必传参数。
3. 配置项：
   1. producer.config：加载一个生产者级别相关配置的配置文件。
   2. producer-property：覆盖配置文件中的参数设置。
   3. property：设置消息消费者相关的配置。
      1. parse.key=true：指定每条消息包含有key。
      2. key.separator=' '：指定消息key和消息净荷【payload】之间的分隔符，默认制表符。

## 查看主题各分区对应的偏移量

```shell
kafka-run-class kafka.tools.GetOffsetShell --broker-list ip:port --topic topicName --time -1
```

1. 可以通过partitions参数指定一个或多个分区，多个分区用逗号隔开。
2. time：-1【lastest】，默认；-2【earliest】

## 查看消息

```shell
kafka-run-class kafka.tools.DumpLogSegments --files fileName【.log】 
```

## 性能测试工具

```shell
kafka-producer-perf-test --num-records num --record-size num --topic topicName --throughput num --producer-props 
p1=value1 p2=value2
```

1. num-records：总条数
2. record-size：每条消息的字节数。
3. throughput：限流控制
   1. num > 0：已发送消息总字节数和当前已执行时间的比值大于num，生产者线程阻塞一段时间。
   2. num = 0：生产者在发送一次消息之后，检测满足阻塞条件时，将会一直阻塞。
   3. num<0：不限流。
4. producer-props：指定配置
   1. bootstrap.servers：代理列表。
   2. acks：
      1. 0：生产者能够通过网络把消息发送出去，那么就认为消息已成功写入Kafka， 一定会丢失一些数据。
      2. 1：只保证leader把消息写到kafka，还是可能会丢数据。
      3. all： 所有同步副本都收到消息。

### 压测输出字段

```shell
100000 records sent, 30543.677459 records/sec (29.13 MB/sec), 794.43 ms avg latency, 1228.00 ms max latency, 835 ms 50th, 1114 ms 95th, 1194 ms 99th, 1225 ms 99.9th.
```

1. 100000 records sent：总共发送了10万条消息。
2. 30543.677459 records/sec：吞吐量为每秒30543.677459条消息。
3. 29.13 MB/sec：每秒发送29兆数据。
4. 794.43 ms avg latency：每条消息平均耗时794ms。
5. 1228.00 ms max latency：消息最长耗时1228.00 ms。
6. 835 ms 50th, 1114 ms 95th, 1194 ms 99th, 1225 ms 99.9th：50、95、99分位的消息耗时。

# 消费者基本操作

## 消费消息

### 旧版高级消费者

```shell
kafka-console-consumer --zookeeper ip1:port1,ip2:port2 --topic topicName --consumer-property param=value [--consumer-property param=value] --from-beginning --delete-consumer-offsets
```

1. zookeeper：表示消费者为旧版高级消费者。
2. topic：必传，指定主题。
3. consumer-property：参数后以键值对的形式指定消费者级别的配置。
   1. group.id：设置消费者组名，默认：`console-consumer-100000以内随机整数`。
   2. consumer.id：消费者id。
      1. 设置：启动时在Zookeeper注册该消费者id，Zookeeper会创建一个 `${group.id}-${consumer.id}`的节点。
      2. 不设置：consumer.id为`${hostName}-${timeStamp}-${UUID前8位字符}`。

4. from-beginning：设置从消息起始位置开始消费，默认是最新消息。旧版不支持指定--offset参数。
5. delete-consumer-offsets：删除在Zookeeper中已消费的偏移量，很少使用。
6. new-consumer：表示使用新版消费者。

### 新版本消费者

1. 去掉了对zookeeper的依赖，不再向zookeeper注册，而是由消费者组协调器【GroupCoordinator】统一管理。
2. 消费者已消费消息的偏移量提交后保存到名为【__consumer_offsets】的内部主题中。

```shell
kafka-console-consumer --bootstrap-server ip1:port1,ip2:port2 --topic topicName --consumer-property param=value [--consumer-property param=value]
```

1. bootstrap-server：表示消费者为新版消费者，指定代理列表。
2. consumer-property
   1. group.Id：不指定，属于默认消费组。
   2. client.id：消费者id。

#### 计算消费组已消费的偏移量存储在`__consumer_offsets`的分区数

```sh
Math.abs(${group.id}.hashCode()) % ${offsets.topic.num.partitions}
```

#### 查看`__consumer_offsets`主题指定分区的信息

```shell
kafka-simple-consumer-shell --topic __consumer_offsets --partition num --broker-list ip1:port1,ip2:port2 --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter"
```

### 消费多主题

--whitelsit 指定消费的多个主题，eg："kafka-action|producer-perf-test"，支持正则表达式。

## 查看消费组

1. 查看消费组名列表

   ```shell
   kafka-consumer-groups --bootstrap-server ip1:port1,ip2:port2 --list
   ```

2. 查看消费组消费情况

   ```shell
   kafka-consumer-groups --bootstrap-server ip1:port1,ip2:port2 --describe [--group groupName|--all-groups]
   ```

3. 删除消费组

   ```shell
   kafka-consumer-groups --bootstrap-server ip1:port1,ip2:port2 --delete [--group groupName|--all-groups]
   ```

## 性能测试工具

```shell
kafka-consumer-perf-test --broker-list ip1:port1,ip2:port2 --threads num --messages num --group groupName --topic topicName 
```

1. threads：总线程数。
2. messages：消息总条数。

### 压测输出字段

```shell
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2021-05-31 23:32:22:974, 2021-05-31 23:32:24:402, 190.7349, 133.5679, 200008, 140061.6246, 256, 1172, 162.7431, 170655.2901
```

1. start.time：开始时间。
2. end.time：结束时间。
3. data.consumed.in.MB：消费的消息总量。
4. MB.sec：按消息总量统计的吞吐量，单位为MB/s。
5. data.consumed.in.nMsg：消费的消息总条数。
6. nMsg.sec：按消息总条数统计的吞吐量，单位为条/s。
7. rebalance.time.ms：为消费者分配分区耗时。
8. fetch.time.ms：拉取消息耗费时间。
9. fetch.MB.sec：每秒拉取消息大小。
10. fetch.nMsg.sec：每秒拉取的消息数量，单位为条/s。













