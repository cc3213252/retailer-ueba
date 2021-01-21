package com.blueegg.networkflow_analysis.uv

// 自定义一个布隆过滤器，主要就是一个位图和hash函数
class Bloom(size: Long) extends Serializable {
  private val cap = size // 默认cap应该是2的整次幂

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)  // 每一个位都有了一个不同的权重，越靠前乘越多
    }
    // 返回hash值，要映射到cap范围内
    (cap - 1) & result //截取result本身，避免取余后数据一样
  }
}
