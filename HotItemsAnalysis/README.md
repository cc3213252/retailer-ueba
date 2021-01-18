## 【ok】No ExecutorFactory found to execute the application

flink 1.11以后需要加如下依赖
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

## 测试kafka

```bash
cd zookeeper/conf
cp zoo_sample.cfg zoo.cfg
cd ..
./bin/zkServer.sh start

cd kafka
./bin/kafka-server-start.sh -daemon ./config/server.properties

./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic hotitems
```

1511658000 数字加起来27，这个数能被3整除，结果是8，能被6整除，说明是一个凑整的数  
5分钟统计一次
```text
543462,1715,1464116,pv,1511658000
662867,2244074,1575622,pv,1511658060
561558,3611281,965809,pv,1511658120
894923,1715,1879194,pv,1511658180
834377,2244074,3738615,pv,1511658240
625915,3611281,4339722,pv,1511658300
```
这些完了后还没有触发，事件时间中最后一条正好没算进去，需要再加1就算进去了   
625915,3611281,570735,pv,1511658301  

结果
```text
窗口结束时间：2017-11-26 09:05:00.0NO1: 商品ID = 1715	热门度 = 2
NO2: 商品ID = 2244074	热门度 = 2
NO3: 商品ID = 3611281	热门度 = 1
==============================
```
结果说明，3611281最后两条没算进去，根据左开右闭，时间统计到1511658300为止  

## 加了打印的测试

下面这些输入后只有dataStream流输出
```text
543462,1715,1464116,pv,1511658000
662867,2244074,1575622,pv,1511658060
561558,3611281,965809,pv,1511658120
894923,1715,1879194,pv,1511658180
834377,2244074,3738615,pv,1511658240
```
加一条 625915,3611281,4339722,pv,1511658300 后多了aggStream输出，
说明agg流5分钟时间一到就会统计输出，但是没有触发排序操作，因为代码中叠加了1毫秒    

## 进阶推论

直接输入一条一小时以后的数据，会发生什么  
937166,1715,2355072,pv,1511661600  