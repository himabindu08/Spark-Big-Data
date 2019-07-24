import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object Task1 {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("Task1")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Graphs")
      .config(conf =conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // 1.Import the trip dataset and station dataset as a csv file
    val trip_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP5/Source code/SparkGraphframe/SparkGraphframe/Datasets/Datasets/201508_trip_data.csv")

    val station_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP5/Source code/SparkGraphframe/SparkGraphframe/Datasets/Datasets/201508_station_data.csv")

    // Printing the Schema
    trip_data.printSchema()
    station_data.printSchema()

    // create or replace Temp View
    trip_data.createOrReplaceTempView("Trip")
    station_data.createOrReplaceTempView("Station")

    val n_station = spark.sql("select * from Station")

    val n_trip = spark.sql("select * from Trip")
    
    //2. concatenate chunks into list & convert to DataFrame

    val concat=spark.sql(sqlText="select concat(lat,long) from Stations").show()

    

    //3.Remove duplicates using distinct
    val stationVertices = n_station
      .withColumnRenamed("name", "id")
      .distinct()
    //4. ReNaming the Columns
    val tripEdges = n_trip
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    //5. Output DataFrame
    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + n_trip.count)//

    // 7.Show some vertices
    stationGraph.vertices.show()
    // 8.Show some edges
    stationGraph.edges.show()

    // 9.Vertex in-Degree
    val in_Degree = stationGraph.inDegrees
    println("InDegree" + in_Degree.orderBy(desc("inDegree")).limit(5))
    in_Degree.show(5)

    //10.Vertex out-Degree
    val out_Degree = stationGraph.outDegrees
    println("OutDegree" + out_Degree.orderBy(desc("outDegree")).limit(5))
    out_Degree.show(5)

    //11.Apply the motif findings
    val motifs = stationGraph.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()

    // Bonus: Vertex degree
    val ver = stationGraph.degrees
    println("Degree" + ver.orderBy(desc("Degree")).limit(5))
    ver.show(5)
// Bonus 2
    val topTrips = stationGraph
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)
    println("Top Trips:" + topTrips.show())
    topTrips.show()

    // Bonus 3:

    val degreeRatio = in_Degree.join(out_Degree, in_Degree.col("id") === out_Degree.col("id"))
      .drop(out_Degree.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    degreeRatio.cache()

    println("Diaplay Ratio:" + degreeRatio.orderBy(desc("degreeRatio")).limit(10))
    degreeRatio.show()

    //Bonus 4:Write graph vertices and edges to csv file
    stationGraph.vertices.write.csv("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP5/Source code/SparkGraphframe/SparkGraphframe/Datasets/vertices")

    stationGraph.edges.write.csv("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP5/Source code/SparkGraphframe/SparkGraphframe/Datasets/edges")


  }
}
