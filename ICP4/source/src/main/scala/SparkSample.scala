import org.apache.spark._
import org.apache.log4j._

object SparkSample {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Users\\bindu\\Documents\\Bigdata ICPs\\winutils" )

    //Controling log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Spark Context
    val conf = new SparkConf().setAppName("SparkSample").setMaster("local[1]");
    val sc  = new SparkContext(conf);

    //Read the file
    val textfile = sc.textFile("C:/Users/bindu/Desktop/lorem.txt.txt");

    //Map Function on RDD produce single result per line or single element per line
    val linelength = textfile.map(s => s.length);
    linelength.foreach(println)

    // FlatMap return list of element
    val FlatMapFile = textfile.flatMap(line => line.split(" "));
    FlatMapFile.foreach(println)


    // Filter Transformation

    val FilterMap=FlatMapFile.filter(value => value=="they");

    FilterMap.foreach(println)

    val totalLength = linelength.reduce((a, b) => a + b)

    println("length", totalLength );

    println("Count:",textfile.count());

    sc.stop();

  }

}