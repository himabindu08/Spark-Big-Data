import org.apache.spark._
import org.apache.spark.streaming._

object LogFilesStreaming {

  def main(args: Array[String]): Unit = {

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    val conf = new SparkConf().setMaster("local[2]").setAppName("log")

    //the check pointed data would be rewritten every 10 seconds......checkpoint is nothing but cache but it stores on disk
    val ssc = new StreamingContext(conf, Seconds(1))

    //textFileStream can only monitor a folder when the files in the folder are being added or updated.
    val lines = ssc.textFileStream("C:/Users/bindu/Documents/Bigdata ICPs/Module2/ICP4/SparkStreamingScala/SparkStreamingScala/Logs")

    val wc = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey( _+ _)
   // println(lines)
    wc.print()
    ssc.start()
    ssc.awaitTermination()


  }
}