import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object rddAuthor extends java.lang.Object {

  case class Libro(idLibro: Int, titolo: String, numeroPagine: Int, copertina: String, idAuthor: Int)

  case class Autore(idAutore: Int, nome: String, cognome: String, eta: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("SparkEx")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val author1 = Autore(1, "Stephen", "King", 55)
    val author2 = Autore(2, "J.K.", "Rowling", 60)
    val libro1 = Libro(1, "il silenzio degli innocenti", 565, "copertina1", 1)
    val libro2 = Libro(2, "harry potter e la pietra filosofale", 356, "copertina2", 2)
    val libro3 = Libro(3, "it", 800, "copertina3", 1)
    val libro4 = Libro(4, "harry potter e la camera dei segreti", 400, "copertina4", 2)

    val tupla1 = (author1, libro1)
    val tupla2 = (author2, libro2)
    val tupla3 = (author1, libro3)
    val tupla4 = (author2, libro4)

    val superTupla = Array(tupla1, tupla2, tupla3, tupla4)

    val rdd = sc.parallelize(superTupla)
    val resultRdd = rdd.map(x => (x._1.nome, x._2)) //prende il nome dell'autore e crea un altro rdd con nome e Libro
    val max = resultRdd.reduceByKey((k, v) => {
      if (k.numeroPagine > v.numeroPagine) k else v
    }) //prende il libro con maggior numero di pagine
    max.foreach(println)
    val totpag = resultRdd.aggregateByKey(0)((x, y) => (x + y.numeroPagine), (x, y) => x + y) //prende la somma delle pagine per autore
    }
}