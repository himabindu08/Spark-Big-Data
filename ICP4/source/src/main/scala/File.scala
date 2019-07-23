import scala.io.{Source}
import java.io.{File, PrintWriter}


object File {

  def main(args: Array[String]):Unit = {
    var a = 1
    while (a <=100) {
      //Data to write in File using PrintWriter
      val writer = new PrintWriter(new File("C:/Users/bindu/Documents/Bigdata ICPs/Module2/ICP4/SparkStreamingScala/SparkStreamingScala/Logs/log"+a+".txt"))

      //Read the data from text file
      val filename = "lorem.txt.txt"
      // For loop to write the lines per batch in logs
      for (line <- Source.fromFile(filename).getLines) {
        writer.write(line)
        writer.write("\n")

      }
      Thread.sleep(5000)
      a = a+ 1
      writer.close()

    }
  }
}