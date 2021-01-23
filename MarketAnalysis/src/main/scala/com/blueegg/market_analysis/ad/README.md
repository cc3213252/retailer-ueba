

```text
Error:(31, 20) not enough arguments for constructor OutputTag: (implicit evidence$1: org.apache.flink.api.common.typeinfo.TypeInformation[com.blueegg.market_analysis.ad.BlackListUserWarning])org.apache.flink.streaming.api.scala.OutputTag[com.blueegg.market_analysis.ad.BlackListUserWarning].
Unspecified value parameter evidence$1.
        ctx.output(new OutputTag[BlackListUserWarning]("warning"), BlackListUserWarning(value.userId, value.adId, "Click ad over" + maxCount + "times today."))
```

还是隐式转换的问题，原import org.apache.flink.streaming.api.scala.OutputTag 改 import org.apache.flink.streaming.api.scala._