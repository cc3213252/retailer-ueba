package com.blueegg.loginfail_detect.cep

import java.util

import com.blueegg.loginfail_detect.{LoginEvent, LoginFailWarning}
import org.apache.flink.cep.PatternSelectFunction

// 实现自定义PatternSelectFunction
class LoginFailEventMatch2() extends PatternSelectFunction[LoginEvent, LoginFailWarning]{
  // 这个map的key是前面pattern的名字
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    // 当前匹配到的事件序列，就保存在Map里
    val firstFailEvent = pattern.get("firstFail").get(0)
    val thirdFailEvent = pattern.get("thirdFail").iterator().next()
    LoginFailWarning(firstFailEvent.userId, firstFailEvent.timestamp, thirdFailEvent.timestamp, "login fail")
  }
}
