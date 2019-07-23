import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object GraphX {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("GraphX")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("GraphX")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    //1.  Importing CSV files
    val trips_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP6/Source code/SparkGraphframe/Datasets/201508_trip_data.csv")

    val station_data = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP6/Source code/SparkGraphframe/Datasets/201508_station_data.csv")

    // Printing the Schema
    trips_data.printSchema()
    station_data.printSchema()

    // create  Temp View
    trips_data.createOrReplaceTempView("Trips")
    station_data.createOrReplaceTempView("Stations")


    val nstation = spark.sql("select * from Stations")
    val ntrips = spark.sql("select * from Trips")

    val stationVertices = nstation
      .withColumnRenamed("name", "id")
      .distinct()

    val tripEdges = ntrips
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("End Station", "dst")

    val stationGraph = GraphFrame(stationVertices, tripEdges)

    tripEdges.cache()
    stationVertices.cache()

    println("Total Number of Stations: " + stationGraph.vertices.count)
    println("Total Number of Distinct Stations: " + stationGraph.vertices.distinct().count)
    println("Total Number of Trips in Graph: " + stationGraph.edges.count)
    println("Total Number of Distinct Trips in Graph: " + stationGraph.edges.distinct().count)
    println("Total Number of Trips in Original Data: " + ntrips.count)//

    stationGraph.vertices.show()
    stationGraph.edges.show()


    // Triangle Count

    val stationTraingleCount = stationGraph.triangleCount.run()
    stationTraingleCount.select("id","count").show()

    // Shortest Path
    val shortPath = stationGraph.shortestPaths.landmarks(Seq("Clay at Battery", "Davis at Jackson")).run
    shortPath.orderBy("id").show(20,false)

    //Page Rank

    val stationPageRank = stationGraph.pageRank.resetProbability(0.15).tol(0.01).run()
    stationPageRank.vertices.show()
    stationPageRank.edges.show()

    // Run PageRank for a fixed number of iterations.
    val results2 = stationGraph.pageRank.resetProbability(0.15).maxIter(10).run()
    results2.vertices.show()
    results2.edges.show()

    // Run PageRank personalized for vertex "a"
    val results3 = stationGraph.pageRank.resetProbability(0.15).maxIter(10).sourceId("Davis at Jackson").run()
    results3.vertices.show()
    results3.edges.show()

   //Label propagation algorithm LPA
    val result = stationGraph.labelPropagation.maxIter(5).run()
    result.orderBy("label").show(false)

    // BFS
    val pathBFS = stationGraph.bfs.fromExpr("id = 'Harry Bridges Plaza (Ferry Building)'").toExpr("dockcount = 23").run()
    pathBFS.show(false)


    //Saving to File
    stationGraph.vertices.write.csv("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP6/Source code/SparkGraphframe/Datasets/vertices")
    stationGraph.edges.write.csv("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP6/Source code/SparkGraphframe/Datasets/edge")



  }

}