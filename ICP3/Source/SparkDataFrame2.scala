import org.apache.spark._


import org.apache.spark.sql.Row

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

object SparkDataFrame2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("sdf").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    System.setProperty("hadoop.home.dir", "C:\\Users\\bindu\\Documents\\Bigdata ICPs\\winutils")



    //spark context
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._
    //Importing the data set and read the file
    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/Sravanthi Somalaraju/Documents/Bigdata ICPs/Module2/ICP3/survey.csv")



    //Creating dataframes
    //df.show(numRows = 1000)
    val df1 = df.limit(5)
    df1.show()
    val df2 = df.limit(10)
    df2.show()


    // using union
    val unionDf = df1.union(df2)
    unionDf.show()

    //Save data to file
    println("\n Saved data to file")
    unionDf.write.parquet("/Users/Sravanthi Somalaraju/Documents/Bigdata ICPs/Module2/ICP3/SparkDataFrame1/SparkDataFrame/temp.parquet")

    // OrderBy with column Country
    unionDf.orderBy("Country").show()

    //creating temp dataframe
    df.createOrReplaceTempView("person")

    //query to find duplicate records
    val DupDF = spark.sql("select COUNT(*),Country from person GROUP By Country Having COUNT(*) > 1")
    DupDF.show()

    //query based on group by using treatment column
    val treatment = spark.sql("select count(Country),treatment from person GROUP BY treatment ")
    treatment.show()

    //13th Row from DataFrame
    val df13th = df.take(13).last
    print(df13th)

    //aggregate
    //Aggregate Max and Average
    val MaxDF = spark.sql("select Max(Age) from person")
    MaxDF.show()

    val AvgDF = spark.sql("select Avg(Age) from person")
    AvgDF.show()

    //join query
    // To create DataFrame using SQLContext

    val department = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/Sravanthi Somalaraju/Downloads/survey2.csv")

    //println("sravs")
    //department.show(100,true)
    val people = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/Sravanthi Somalaraju/Downloads/survey1.csv")
   // people.show(100, true)

    //creating temp dataframes to perform join
    people.createOrReplaceTempView("data")
    department.createOrReplaceTempView("info")

    val joinSQL = spark.sql("SELECT data.Country,data.Age,info.coworkers,info.leave FROM data,info where data.Timestamp = " +
      "info.Timestamp")
    joinSQL.show()
    //Inner Join
    val joinRight = spark.sql("SELECT data.*,info.* FROM data INNER JOIN info ON(data.timestamp = info.timestamp)")
    joinRight.show()

    //printing schema
    println("\n Printing schema of stored data")
    df.printSchema()

    //Bonus
    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val C1 = fields(1).toString
      val C2 = fields(2).toString
      val C3 = fields(3).toString
      (C1,C2, C3)
    }

    val lines = sc.textFile("/Users/bindu/Documents/Bigdata ICPs/Module2/ICP3/survey.csv")
    val rdd = lines.map(parseLine).toDF()

    rdd.show()

  }
}