## WindowFunction的参数如何确定

WindowFunction[IN, OUT, KEY, W <: Window]
OUT输出是样例类类型 PageViewCount  
KEY这个是看keyBy的结果的，如果定义String类型的url，那么取数据就像热门商品一样很麻烦
（val itemId = key.asInstanceOf[Tuple1[Long]].f0）
keyBy还有一个方法是自定义啥输出啥，故定义成_.url，输出是String，这里就用String  

## KeyedProcessFunction的参数如何确定

同上，keyBy用_.windowEnd，windowEnd是啥第一个参数key就是啥，是Long  
输入 PageViewCount  
输出 String  

## 正则 ^((?!\\.(css|js)$).)*$

?! 不以  
.(css|js)$ 这两个扩展名结尾  
^ 开头
*$ 任意结尾  

## HotPagesNetworkFlow 这个项目watermark定了1分钟，到底定多少合适

看数据乱序程度，把watermark和窗口延迟触发机制一起处理，watermark设置小一点，窗口多等一会
如果还有没处理完的数据，放到侧输出流  

## 测试 HotPagesNetworkFlowWatermark

nc -lk 7777  
83.149.9.216 - - 17/05/2015:10:25:49 +0000 GET /presentations
83.149.9.216 - - 17/05/2015:10:25:50 +0000 GET /presentations  这里watermark是49，故没有窗口关闭
83.149.9.216 - - 17/05/2015:10:25:51 +0000 GET /presentations  输出聚合操作，watermark是50，加1才会触发定时器，故没有排序信息输出
83.149.9.216 - - 17/05/2015:10:25:52 +0000 GET /presentations

测乱序数据：  
83.149.9.216 - - 17/05/2015:10:25:46 +0000 GET /presentations  在之前的聚合窗口上叠加，每来一个PageViewCount就会注册一个定时器，过去的
数据不会更新watermark（watermark对于过去的数据是不更新的），故不会触发定时器  

83.149.9.216 - - 17/05/2015:10:25:53 +0000 GET /presentations  更新了watermark，故上面那条就可以生效了  

测迟到数据：  
83.149.9.216 - - 17/05/2015:10:25:31 +0000 GET /presentations  因定义了1分钟迟到数据，故只要1分钟以内的都会输出，故输出很多窗口聚合结果

## 扩展

83.149.9.216 - - 17/05/2015:10:25:23 +0000 GET /presentations  
83.149.9.216 - - 17/05/2015:10:25:54 +0000 GET /presentations  watermark是53，50这个窗口关了
83.149.9.216 - - 17/05/2015:10:25:51 +0000 GET /presentations  输出到侧输出流

## HotPagesNetworkFlowWatermark这个程序状态bug产生的原因

```text
窗口结束时间：2015-05-17 10:25:45.0
NO1: 页面URL = /presentations	热门度 = 2
NO2: 页面URL = /presentations	热门度 = 1
```
定时器里面，只对windowEnd分组了，没有对url分组，迟到数据来一条会处理一条