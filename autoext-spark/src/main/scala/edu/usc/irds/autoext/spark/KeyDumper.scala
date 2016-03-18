package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Writable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

/**
  * Dumps all the keys of sequence files
  */
class KeyDumper extends CliTool {

  @Option(name = "-list", required = true, usage = "Path to a file having list of input files")
  var listFile: String = null

  @Option(name = "-out", required = true, usage = "Path to output file")
  var output:String = null

  def run(): Unit ={
    val ctx = new SparkContext(new SparkConf().setAppName("Key Dumper"))

    val paths = ctx.textFile(listFile).collect()
    val rdds = new Array[RDD[(Writable, Writable)]](paths.length)
    for(i <- paths.indices) {
      rdds(i) = ctx.sequenceFile(paths(i), classOf[Writable], classOf[Writable])
    }
    KeyDumper.LOG.info(s"Read ${rdds.length} paths from $listFile")
    ctx.union(rdds)
      .map(rec => rec._1.toString) //keys only
      .saveAsTextFile(output) //write it to a file
    ctx.stop()
    KeyDumper.LOG.info(s"Stored the output at $output")
  }
}

object KeyDumper{
  val LOG = LoggerFactory.getLogger(KeyDumper.getClass)

  def main(args: Array[String]) {
    val i = new KeyDumper
    i.parseArgs(args)
    i.run()
  }
}