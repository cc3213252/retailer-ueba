package com.blueegg.loginfail_detect

// 输入登陆事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, timestamp: Long)
