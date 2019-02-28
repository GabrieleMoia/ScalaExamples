import java.sql.Date

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Spark extends java.lang.Object {

  case class Libro(idLibro: Int, titolo: String, numeroPagine: Int, copertina: String, idAuthor: Int, data: java.sql.Date)

  case class Autore(idAutore: Int, nome: String, cognome: String, eta: Int)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("SparkEx")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._

    val author1 = Autore(1, "Stephen", "King", 55)
    val author2 = Autore(2, "J.K.", "Rowling", 60)
    val libro1 = Libro(1, "il silenzio degli innocenti", 565, "copertina1", 1, Date.valueOf("2018-04-10"))
    val libro2 = Libro(2, "harry potter e la pietra filosofale", 356, "copertina2", 2, Date.valueOf("2018-05-01"))
    val libro3 = Libro(3, "it", 800, "copertina3", 1, Date.valueOf("2018-06-01"))
    val libro4 = Libro(4, "harry potter e la camera dei segreti", 400, "copertina4", 2, Date.valueOf("2018-07-01"))
    val libro5 = Libro(5, "harry potter e il prigioniero di azkaban", 425, "copertina5", 2, Date.valueOf("2018-08-01"))
    val libro6 = Libro(6, "shining", 850, "copertina6", 1, Date.valueOf("2018-09-01"))

    val autori = Array(author1, author2)
    val libri = Array(libro1, libro2, libro3, libro4, libro5, libro6)


    val rddAuthor = sc.parallelize(autori)
    val rddBooks = sc.parallelize(libri)
    val dfAuthor = rddAuthor.toDF()
    val dfBooks = rddBooks.toDF()

    val df = dfAuthor.join(dfBooks, dfAuthor("idAutore") === dfBooks("idAuthor")).drop("idAuthor")

    /*val dfGroup = df.groupBy("idAutore").max("numeroPagine")
    val dfSum = df.groupBy("idAutore","nome").agg(max(length($"titolo")))*/

    val def_max = df.select($"idAutore", $"titolo", $"numeroPagine", max($"numeroPagine").over(Window.partitionBy("idAutore")).as("maxPages")).withColumn("diffPages", $"maxPages" - $"numeroPagine")
    val def_date = df.select($"idAutore", $"titolo", $"numeroPagine", lag($"titolo", 1).over(Window.partitionBy($"idAutore").orderBy($"data")) as "prec", lead($"titolo", 1).over(Window.partitionBy($"idAutore").orderBy($"data")) as "succ")

    def_max.show()
    def_date.show()

    val df_max = df.select("*").groupBy("numeroPagine").max("numeroPagine").withColumn("diff", max("numeroPagine")-$"numeroPagine")
    df_max.show()
    /*val windowSpec= Window.partitionBy("nome").orderBy("numeroPagine")
    val max = df.select("nome", "numeroPagine").(windowSpec)*/
  }
}