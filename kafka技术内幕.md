## 概念

- **kafka集群**：由多个消息代理服务器组成。

- **消息代理服务器【broker server】**：服务器节点。

- **分区【partition】**：最小的粒度，将分区分配给消费组不同的而且是唯一的消费者，并确保一个分区只属于一个消费者。

- **主题【topic】**：消息的类别，不同应用产生不同类型的数据，建议设置不同的主题。一个主题一般会有多个消费者的订阅。从物理意义上可以把主题看做分区的日志文件。

- **分区日志文件**：kafka集群为每个主题维护了分布式的分区日志文件。原始的消息内容和分配的偏移量以及其他一些元数据信息都会存储到分区日志文件中。

- **提交日志【commit log】**：每个分区都有一个有序，不可变的记录序列，新的消息会不断追加到提交日志。

- **偏移量【offset】**：分区中的每条消息都会按时间顺序分配到一个单调递增的顺序标号，即偏移量。

- **消息**：包括key，消息内容和时间戳。消息有键值时，通过键值分布到同一个分区，没有键值时，通过轮询的方式发到分区。

## 分区模型

<img src="imgs/分区模型.png" alt="image-20230805162833151" style="zoom:50%;" />

一个主题有多个分区，每个分区只能是一个消费组的一个消费者消费，保证了消息的有序性，且同一个消费组不同消费者只能消费不同的分区，保证了消息的负载均衡。

## 消费模型

<img src="/Users/sunchen/Library/Application Support/typora-user-images/image-20230805163020685.png" alt="image-20230805163020685" style="zoom:50%;" />

拉取模式，消费者通过控制偏移量来消费消息。

不管消息有没有被消费，消息会一直存储在kafka集群中。

过期策略：设置保留时间。

## 分布式模型

每个分区都会以副本的方式复制到多个消息代理节点上。其中一个节点作为主副本【leader】。其他节点作为备份副本【follower】

主副本【leader】：负责所有客户端的读写操作。

备份副本【follower】：从主副本同步数据。

## 设计与实现

### 文件系统的持久化

**操作系统概念**

**预读**：提前将一个比较大的磁盘块读入内存。

**后写**：将很多很小的逻辑写操作合并起来组合成一个大的物理写操作。

**磁盘缓存：**操作系统将主内存剩余的所有空闲内存空间用作磁盘缓存。

<img src="./imgs/文件存储.png" alt="image-20230805172515375" style="zoom:50%;" />

kafka在存储数据时，所有的数据立即写入文件系统的持久化日志文件，首先到磁盘缓存，操作系统随后将这些数据定期自动刷新到物理磁盘。

### 数据传输效率

**零拷贝技术**：只需将磁盘文件的数据复制到页面缓存中一次，然后将数据从页面缓存直接发送到网络中。

<img src="/Users/sunchen/Documents/mystudy/mymiddleware/imgs/零拷贝技术.png" alt="image-20230805173133141" style="zoom:50%;" />



### 生产者



### 消费者

<img src="/Users/sunchen/Documents/mystudy/mymiddleware/imgs/拉模式.png" alt="image-20230805173953153" style="zoom:50%;" />

**消费状态处理机制：**

1. 消费者记录每个分区的消费进度（偏移量）
2. 消费者会定时的将分区的消费进度保存成检查点文件，表示这个位置之前的消息已经被消费过了。

**消费者批量拉消息机制：**

永远处于消费状态，如果消息较少时，可以采用阻塞式，长轮询的方式等待。

### 副本机制

在多个服务器节点上对每个主题分区的日志进行复制。

**备份副本从主副本拉消息的过程：**

1. 消费方式和普通消费者一样
2. 备份副本会将消息运用到自己的本地日志文件。

### 容错机制

**节点存活状态判断**：

- 节点必须和 ZK 保持会话
- 如果这个节点是某个分区的备份副本，它必须对分区主副本的写操作进行复制，复制的进度不能落后太多。

满足上面两个条件，叫做“正在同步中”【in-sync】

**ISR**：每个分区的主副本会跟踪正在同步中的备份副本节点。

**已经提交的消息**：一条消息只有被ISR 集合中的所有副本都运用到本地的日志文件，才算成功提交了。已经提交的消息才能被消费者消费。

## KAFKA命令

**启动**

```shell
# 启动zookeepe
zookeeper-server-start -daemon /usr/local/etc/kafka/zookeeper.properties

# 启动kafka实例
kafka-server-start -daemon /usr/local/etc/kafka/server_1.properties
kafka-server-start -daemon /usr/local/etc/kafka/server_2.properties
kafka-server-start -daemon /usr/local/etc/kafka/server_3.properties
kafka-server-start -daemon /usr/local/etc/kafka/server_4.properties

# 启动EFAK
ke.sh start
```

**主题**

```shell
# 创建主题，副本数不能多于broker的数量
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic {topicname}

# 主题列表
kafka-topics --list --bootstrap-server localhost:9092

# 主题详情
kafka-topics --describe --bootstrap-server localhost:9092 --topic {topicname}

# 删除主题
kafka-topics --delete --bootstrap-server localhost:9092 --topic {topicname}

# 修改主题，只可以增加分区数量
kafka-topics --alter --bootstrap-server localhost:9092 --partitions 1 --topic {topicname}
```

**生产者**

```shell
kafka-console-producer --broker-list localhost:9092 --topic {topicname} 
```

**消费者**

```shell
kafka-console-consumer --bootstrap-server localhost:9092 --topic {topicname} --from-beginning
```

# 客户端

## 生产者客户端

1. 当集群的实例变化时及时更新生产者客户端的数据。【**分区、记录收集器、发送线程**】
2. 定时将记录收集器的数据传递给发送线程。

### 元数据【Metadata】

向任意实例发送 `metadata` 请求。拿到元数据。

```go
type MetadataResponse struct {
	Brokers []   *meta.Broker
	ClusterID    *string
	ControllerID int32
	Topics       []*meta.TopicMetadata
}

// Broker 实例信息
type Broker struct {
	ID            int32
	Addr          string
	CorrelationID int32
    Conn          net.Conn
}

// TopicMetadata 主题的内容
type TopicMetadata struct {
    ID         int32
	Name 	   string
	IsInternal bool
	Partitions []*PartitionMetadata
}

// PartitionMetadata 包含主题中的每个分区。
type PartitionMetadata struct {
	ID int32
	Leader int32
	Replicas []int32
	Isr []int32
	OfflineReplicas []int32
}
```

### 控制器【Control】

1. **选择分区【Partition】**

   需要保证消息均衡地分布到各个服务端节点。

   - 消息有键：通过计数器自增轮询的方式依次将消息分配到不同的分区上。
   - 消息无键：对键计算散列值，然后和主题的分区数进行取模得到分区编号。

2. **追加到记录收集器中**。收集器满了后通知发送线程。

### 记录收集器【RecordAumulator】

作用是缓存客户端的消息，需要通过消息发送线程才能将消息发送到服务端。

1. 获取**分区所属的队列**。取队列中最后一个批记录。
2. 如果队列中**不存在批记录**或**批记录已满**，创建新的批记录，加在队列尾部。
3. 当批记录满时，从队列头部弹出批记录，将批记录**发给发送线程**。

### 发送线程【Sender】

1. 设置指定时间内收集不超过指定数量的消息。
2. 如果指定时间内达到指定数量，则立即发送，如果没有达到，也要把消息发出去。
3. 不负责真正发送客户端请求，从记录收集器中取出要发送的消息，创建好客户端请求，交给网络连接对象。
   1. 获取分区批记录队列中的第一个批记录。  `queue -> map[topicName]map[partitionNum]批记录`。
   2. 将相同主副本节点的所有批记录放在一起。  `NodeID -> map[topicName]map[partitionNum]批记录`。
   3. 将消息交给网络连接管理器。

### 网络连接管理器【NetworkClient】

管理了客户端和服务端之间的网络通信。包括连接的建立、发送客户端请求、读取客户端响应。

1. 连接建立后，将客户端请求加入inFlightRequests列表中，等待发送。【**针对同一个服务端，如果上一个客户端请求还没有发送完成，则不允许发送新的客户端请求**】。
2. 建立起请求和相应的映射关系，等响应回来后，能够找到对应的消息内容。
3. 按序向服务器发送请求信息。
4. 收到响应后，使用响应结果来回调程序自定义的回调函数。

## 消费者客户端

### 协调者【Coordinator】

每个消费组固定一个协调者，存储在`__consumer_offsets-*`中，作为心跳、提交偏移量、加入组的目标broker。

- **寻找协调者【FindCoordinator】**：向任意一个实例发送协调者请求。获取结果。

- **加入组请求【JoinGroup】**：得到分配的 `memberID` 和 `generationID`。

  - ```go
    // JoinGroupRequest 加入组请求
    type JoinGroupRequest struct {
    	GroupId               string           // 组标识
    	SessionTimeout        int32            // 心跳超时时间
    	RebalanceTimeout      int32            // 平衡组时，等待每个成员加入的最长时间
    	MemberId              string           // 消费者成员编号。第一次请求时传空字符串，结果会返回一个memberID。拿到后立即重新请求一次。
    	ProtocolType          string           // 有两种协议类型：**消费者【consumer】和连接器【connect】**。
    	OrderedGroupProtocols []*GroupProtocol // 可以支持的分区分配方式。
    }
    
    // GroupProtocol 组协议
    type GroupProtocol struct {
    	Name     string                    // 协议名称：range：连续分配|roundrobion：循环分配|sticky：尽量少调整改动
    	Metadata *meta.ConsumerGroupMember // 支持协议的topic和分区内容
    }
    
    type ConsumerGroupMember struct {
    	Topics          []string          // topic列表
    	OwnedPartitions []*OwnedPartition // 分区列表
    	GenerationID    int32             // 当前会话生成的ID。
    }
    ```

  - ```go
    // JoinGroupResponse 加入组返回值
    type JoinGroupResponse struct {
    	GenerationId  int32         // 生成的会话ID。
    	GroupProtocol string        // 协调器选择的组协议。
    	LeaderId      string        // 主消费者。
    	MemberID      string        // 协调者分配的成员ID。
    	Members       []GroupMember // 每个组的成员信息。
    }
    
    // GroupMember 组成员信息
    type GroupMember struct {
    	MemberId        string                    // 成员ID
    	Metadata        *meta.ConsumerGroupMember // 组成员元数据。
    }
    ```

- **同步组请求【SyncGroup】**：获取服务端为消费者分配的分区。

  - `主消费者`：主消费者需要实现分配过程，将分配结果【`map[string][]int32`】作为参数发给服务端。
  - `普通消费者`：只需用`memberID`请求。

### 拉取消息【Fetch】

- **初始化提交偏移量**：向*协调者*发送 `OffsetFetch` 请求。
- **将提交偏移量更新为拉取偏移量**。
- **拉取消息**：使用提交偏移量向分区的主副本节点发送 `Fetch` 请求，本地维护拉取偏移量。
- **消费消息**：消费者实际消费后，本地更新提交偏移量，等待偏移量自动提交。

### 定时任务

#### 心跳任务【Heartbeat】

- **运行**

  - 向*协调者*请求，参数为`memberID、generationID、groupID`。

  - 由客户端轮询控制，每次只有一个心跳请求在运行。

  - 返回结果后，生成下一次的心跳任务。

- **心跳状态**

  - **协调者挂掉了**：客户端下次轮询需要重新获取消费者。
  - **需要重新加入组**：一般为发生再平衡了。

- **再平衡**：变更消费者、变更分区、变更实例等。

#### 提交偏移量【OffsetCommit】

- 向*协调者*请求。
- 再平衡时需要同步提交一次偏移量。
- 每次只有一个提交请求，请求完成后，再生成下次任务。

# 服务端

## 协调者



