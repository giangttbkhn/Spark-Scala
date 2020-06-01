import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCountScala")
//    conf.set("spark.master","spark://dw-test-37156:7077")
    conf.set("spark.master","yarn")
//    conf.set("spark.submit.deployMode","cluster")

    conf.set("spark.executor.memory","3841m")
    conf.set("spark.executor.cores","8")
    conf.set("spark.executor.instances","2")
    conf.set("spark.memory.fraction","0.8")
    conf.set("spark.driver.memory","2048m")
    conf.set("spark.driver.core","1")
    conf.set("spark.yarn.am.memory","1024m")
    conf.set("spark.yarn.am.core","1")

    // Tạo Spark Context.
    val sc = new SparkContext(conf)
    // Load input data
    val input =  sc.textFile(inputFile)
    // Cắt từng dòng thành các
    val words = input.flatMap(line => line.split(" "))
    // Đếm
    val counts = words.map(word => (word, 1)).reduceByKey((x, y) => x + y,1)
    // Lưu kết quả vào file ouput. Đồng thời gọi đánh giá RDD.
    counts.saveAsTextFile(outputFile)
  }
}