package com.blueegg.orderpay_detect.tx

// 定义到账事件样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)
