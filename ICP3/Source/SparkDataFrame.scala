import org.apache.spark.sql.SparkSession

object SparkDataFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val df = spark.read.json("D:\\Big data\\SparkDataframe\\src\\main\\scala\\people.json")

    df.show()
  }
}