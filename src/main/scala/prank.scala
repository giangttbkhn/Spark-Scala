import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object prank {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkPageRank <iter> <file_input> <file_output> ")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("PageRank_Spark_Scala")
//    conf.set("spark.master","spark://dw-test-37156:7077")
    conf.set("spark.master","yarn")
    //    conf.set("spark.submit.deployMode","cluster")

    conf.set("spark.executor.memory","3840m")
    conf.set("spark.executor.cores","8")
    conf.set("spark.executor.instances","2")
    conf.set("spark.memory.fraction","0.8")
    conf.set("spark.driver.memory","2048m")
    conf.set("spark.driver.core","1")
    conf.set("spark.yarn.am.memory","1024m")
    conf.set("spark.yarn.am.core","1")


    val iters = if (args.length > 2) args(0).toInt else 10
    val ctx = new SparkContext(conf)
    val lines = ctx.textFile(args(1))
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey(15).persist(StorageLevel.MEMORY_ONLY)
    var ranks = links.mapValues(v => 1.0)
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.sortByKey(true).saveAsTextFile(args(2))
    ctx.stop()
  }
}
