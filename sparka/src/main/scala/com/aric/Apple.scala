import com.aric.PriceLog
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Apple {
  def main(args: Array[String]): Unit = {
    val sConf = new SparkConf()
    sConf.setAppName("AppleAnalyze")
    sConf.setMaster("local[2]")
    val sCont = new SparkContext(sConf)
    val applePrices = sCont.textFile("file:/Users/dursun/workspaces/spark_ws/data/apple.csv");
    val header = applePrices.take(1)(0)
    val pricesWOHeader: RDD[String] = applePrices.filter(_!=header)
    val priceLogRDD: RDD[PriceLog] = pricesWOHeader.map(_.split(",")).map(PriceLog.fromArray(_))
    priceLogRDD.map(_.adjClose).countByValue().foreach(println);

  }
}