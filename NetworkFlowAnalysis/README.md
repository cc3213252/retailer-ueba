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