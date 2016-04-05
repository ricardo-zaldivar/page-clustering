package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Option

/**
  * Greps the content for specific url sub strings and content type sub strings
  */
class ContentGrep extends IOSparkJob {

  @Option(name = "-urlfilter", usage = "Url filter substring", required = true)
  var urlFilter: String = null

  @Option(name = "-contentfilter", usage = "Content type filter substring")
  var contentFilter:String = null

  def run(): Unit ={
    println("Initializing spark context")
    val paths = getInputPaths()
    println(s"Found ${paths.length} paths")

    val rdds = new Array[RDD[(Text, Content)]](paths.length)
    for( i <- paths.indices){
      rdds(i) = sc.sequenceFile(paths(i), classOf[Text], classOf[Content])
    }
    var rdd = sc.union(rdds)
    val contentFilter = this.contentFilter
    val urlFilter = this.urlFilter
    rdd = rdd.filter(rec => ((urlFilter == null || rec._2.getUrl.contains(urlFilter))
                            && (contentFilter == null || rec._2.getContentType.contains(contentFilter))))
    LOG.info("Saving output at {}", outPath)
    rdd.saveAsHadoopFile(outPath, classOf[Text], classOf[Content], classOf[SequenceFileOutputFormat[_,_]])
    LOG.info("Done. Stopping spark context")
    sc.stop()
  }
}

object ContentGrep {

  def main(args: Array[String]) {
    new ContentGrep().run(args)
  }
}