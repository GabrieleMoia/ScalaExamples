import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainChef {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("Chef")

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    val chef1 = new chef(1, "Antonino", "Cannavacciuolo", 36)
    val chef2 = new chef(2, "Carlo", "Cracco", 40)
    val chef3 = new chef(3, "Bruno", "Barbieri", 48)
    val chef4 = new chef(4, "Giorgio", "Locatelli", 46)

    val ricetta1 = new recip(1, "Pappardelle al ragù", 5, 3)
    val ricetta2 = new recip(2, "Linguine allo scoglio", 3, 1)
    val ricetta3 = new recip(3, "Risotto alla milanese", 2, 4)
    val ricetta4 = new recip(1, "Uovo all'occhio di bue", 3, 2)

    val chefs = Array(chef1, chef2, chef3, chef4)
    val ricette = Array(ricetta1, ricetta2, ricetta3, ricetta4)

    val rddChef = sc.parallelize(chefs)
    val rddRicetta = sc.parallelize(ricette)

    val rdd = sc.parallelize(Seq(
      (chef1, ricetta2),
      (chef2, ricetta4),
      (chef3, ricetta1),
      (chef4, ricetta3)
    ))

    rdd.foreach(println)

    val max = rdd.reduceByKey((k,v) => {if(k.quantità > v.quantità) k else v})
    println(max.first())
  }

}
