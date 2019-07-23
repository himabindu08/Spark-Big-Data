import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Users\\bindu\\Documents\\Bigdata ICPs\\winutils" )
    // Create a local StreamingContext with two working thread and batch interval of 5 second.
    // The master requires 2 cores to prevent a starvation scenario.
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val data = ssc.socketTextStream("10.114.2.8",9999)
   // val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    // Split each line into words
    //val words = data.flatMap(_.split(" "))
    // Count each word in each batch
    val wc = data.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    // Print the first ten elements of each RDD generated in this DStream to the console
    wc.print()
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}