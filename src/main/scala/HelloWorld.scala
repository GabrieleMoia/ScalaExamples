import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main (args:Array[String]): Unit ={

    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("SparkEx")

    val sc = new SparkContext(conf)
    val streamc = new StreamingContext(sc, Seconds(5))

    streamc.checkpoint("D:/error_log/checkpoint")

    val filestream = streamc.textFileStream("D:/error_log")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss")

    val logs = filestream.flatMap(line => {
      val lineSplitted = line.split(",")
      List(Log(new Timestamp(dateFormat.parse(lineSplitted(0)).getTime),
        lineSplitted(1), lineSplitted(2).toInt))
    })

    val logsPair = logs.map(l => (l.tag,1L))
    val result = logsPair.reduceByKey((l1,l2)=> l1 + l2)

    val updateStateFunction = (seqVal : Seq[Long],stateOpt : Option[Long]) => {
      stateOpt match {
        case Some(state) => Option(seqVal.sum + state)
        case None => Option(seqVal.sum)
      }
    }

    logsPair.reduceByKeyAndWindow((l1,l2) => l1 + l2, Seconds(20))
    val logsPairTotal  = logsPair.updateStateByKey(updateStateFunction)
    val joinedLogsPair = result.join(logsPairTotal)

    joinedLogsPair.repartition(1).saveAsTextFiles("D:/error_log/resultLog/","txt")

    streamc.start()
    streamc.awaitTermination()
  }

  def helloWorld():String ={
    val hello:String ="Hello World"
    return hello
  }


  //    import org.apache.spark.streaming._
  //    val mapStateFunction = (key: Int, valueOpt: Option[Long], state:
  //    State[Long]) => {
  //      var total = valueOpt.getOrElse(0.toLong)
  //      if (state.exists()) {
  //        val stateVal = state.get()
  //        total += stateVal
  //      }
  //      state.update(total)
  //      Some(key, total)
  //    }
  //   val logsPairTotalMap = logsPair.mapWithState(StateSpec.function(mapStateFunction)).stateSnapshots()

}
