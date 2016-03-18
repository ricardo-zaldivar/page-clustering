package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.nutch.protocol.Content
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

/**
  * Created by tg on 3/15/16.
  */
class ContentGrep extends CliTool {

  @Option(name = "-list", usage = "Path to List File", required = true)
  var listFile: String = null

  @Option(name = "-urlfilter", usage = "Url filter regex", required = true)
  var urlFilter: String = null

  @Option(name = "-contentfilter", usage = "Content type filter regex", required = true)
  var contentFilter:String = null

  @Option(name = "-out", usage = "Path to output file", required = true)
  var outpath:String = null

  def run(): Unit ={
    println("Initializing spark context")
    val spkCnf = new SparkConf().setAppName(getClass.getName)
    val ctx = new SparkContext(spkCnf)
    val paths = ctx.textFile(listFile).collect()
    println(s"Found ${paths.length} paths in list file $listFile")

    val rdds = new Array[RDD[(Text, Content)]](paths.length)
    for( i <- paths.indices){
      rdds(i) = ctx.sequenceFile(paths(i), classOf[Text], classOf[Content])
    }
    var rdd = ctx.union(rdds)
    val contentFilter = this.contentFilter
    val urlFilter = this.urlFilter
    rdd = rdd.filter(rec => ((urlFilter == null || rec._2.getUrl.contains(urlFilter))
                            && (contentFilter == null || rec._2.getContentType.contains(contentFilter))))
    println(s"Saving output at $outpath")
    rdd.saveAsHadoopFile(outpath, classOf[Text], classOf[Content], classOf[SequenceFileOutputFormat[_,_]])
    println(s"Done. Stopping spark context")
    ctx.stop()
  }
}

object ContentGrep {

  val LOG = LoggerFactory.getLogger(ContentGrep.getClass)

  def main(args: Array[String]) {
    val instance = new ContentGrep
    instance.parseArgs(args)
    instance.run()
  }
}