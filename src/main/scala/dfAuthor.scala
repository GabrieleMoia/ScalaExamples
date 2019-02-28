import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object dfAuthor {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("SparkEx")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val arr = (1 to 100).toArray
    val rdd = sc.parallelize(arr)

    val df = rdd.toDF("number")
    val newdf = df.withColumn("pow", $"number" * $"number")
    val dfwithout3 = newdf.filter($"pow"%3!==0)
    dfwithout3.show()
  }
}
