[参考地址](http://shiyanjun.cn/archives/1097.html)

实时统计用户点击量

    根据每个用户的uid，使用spark,kafka和redis实时统计用户的点击量，模拟用户点击后的记录数据，通过kafka客户端工具类发送
    至kafka的topic中，使用spark stream从kafka中将数据读取出来统计数据，将数据写入Redis中

    版本：
        scala-2.10.4
        kafka_2.11-0.10.1.1
        redis-3.0.0
        spark-2.10
