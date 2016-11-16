package com.aric

/**
  * Created by dursun on 11/14/16.
  */
case class PriceLog(val date: String, val open: String, val high: String, val low: String,
                    val close: String, val volume: String, val adjClose: String)

object PriceLog {
  def fromArray(ps: Array[String]): PriceLog = new PriceLog(ps(0),ps(1),ps(2),ps(3),ps(4),ps(5),ps(6))
}