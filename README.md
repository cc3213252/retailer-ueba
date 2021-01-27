# 电商用户行为分析

资料： 
https://www.bilibili.com/video/BV1Qp4y1Y7YN?p=101

| 软件  | 版本 | 
| ------ | ----   | 
| flink  | 1.12.0   |  
| kafka  | 2.4.1    |  
| scala  | 2.11     |

## 热门商品项目进阶

1、HotItems  
统计最近一小时，按pv统计top 5商品，5分钟统计一次   

2、HotItemsKafka  
数据源改kafka  

3、KafkaProducerUtil    
改成脚本发送kafka数据  

4、HotItemsWithSql  
使用table方式实现（table + sql）  

5、HotItemsWithPureSql  
纯sql方式实现    

## 热门流量统计

1、HotPagesNetworkFlow  
统计10分钟内，访问前三的url，5秒统计一次  
数据时间格式转换  

2、HotPagesNetworkFlowFilter  
过滤掉.css和.js结尾的日志  

3、HotPagesNetworkFlowWatermark  
优化处理效率，正确设置watermark和处理迟到数据  

4、TopNHotPagesFixStatus  
解决topN中url重复问题  

## pv统计

1、PageView  
统计每小时访问条数  

2、PageViewKey  
解决并行度无法起作用问题  

3、PageViewKeyEnd  
解决中间结果刷屏问题  

## uv统计

1、UniqueVisitor  
统计每小时独立用户数  

2、UvWithBloom  
布隆过滤器解决set操作内存可能爆掉问题  

## app市场推广统计

1、AppMarketByChannel  
分渠道市场统计，需要按两个字段分组的场景，process代替aggregate的实现

## 页面广告分析

1、AdClickAnalysis  
统计一天页面广告点击量，和PageView是一样的   

2、AdClickAnalysisFilter  
把刷单用户排除，1小时内下单超过100次报警     

## 恶意登陆检测

1、LoginFail  
2秒内连续两次登陆失败告警  

2、LoginFailAdvance  
时效性做了改进，1秒出现两次失败了就报警  

3、LoginFailWithCep  
cep实现  

4、LoginFailWithCep2  
5秒内有3次失败告警  

5、LoginFailWithCep3  
优化后cep实现  

## 订单支付实时监控

1、OrderTimeOut  
cep实现订单成功支付的和15分钟内未支付超时的  

2、OrderTimeoutWithoutCep  
process实现  

3、TxMatch  
订单流和到账流合流输出，实时对账    

4、TxMatchWithJoin  
合流输出window join实现  