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