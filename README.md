## Kafka 环形队列消费模型

### 传统消费模型:

客户端可以根据多少个分区，然后起多少个消费者，然后根据将拉取的消息循环进行业务处理。

![传统消费模型](http://ww1.sinaimg.cn/large/005RZJcZgy1ge0e4exz9zj30qz0ezwfc.jpg)

显而易见，传统消费模型有个明显的弊端，由于受Kafka限制，partition只能被一个消费者消费，所以这个种消费模型并发度受限于
Kafka partition个数，并且消费速度受限于后端业务处理速度。



### 环形队列消费模型：

客户端同样的可以起一定数量的消费者，由于kafka拉取消息的速度很快，主要限制于后面业务的处理速度，所以为了可以改进这种弊端，我们引入线程池。但是由于线程池调度问题，Kafka offset提交就会存在问题。
因为可能offset大的消息后于offset小的消息被处理，当commit的时候，就会覆盖大的offset消息，这里就会导致下次poll的时候，重复消费。

因此我们需要引入一个环形队列来，控制消息offset的提交时机，当offset大的先执行完，然后对消息标记(并不提交)，当offset小的执行完毕的时候，等待下次poll()方法调用的时候，对队列里面的消息进行提交。
遍历队列的头直到下一个没有被ack的消息，然后提交这个没有被ack消息的前一条消息即为可以提交的最大消息。

![环形队列消费模型](http://ww1.sinaimg.cn/large/005RZJcZgy1ge0e6rsjb6j30p70fkmy4.jpg)


### 环形队列消费模型 vs 传统消费模型

|消费模式|实现难度|并发度|
|---|---|---|
|环形队列模型|难|高|
|传统消费模型|简单|低，受限于partition数目|

## 总结

申请IDEA项目

