package edu.usc.irds.autoext.spark

import org.apache.hadoop.io.Writable

/**
  * Dumps all the keys of sequence files
  */
class KeyDumper extends IOSparkJob {

  def run(): Unit ={
    sc.union(getInputPaths().map(sc.sequenceFile(_,
      classOf[Writable], classOf[Writable])))
      .map(rec => rec._1.toString) //keys only
      .saveAsTextFile(outPath) //write it to a file
    LOG.info(s"Stored the output at $outPath")
  }
}

object KeyDumper{

  def main(args: Array[String]) {
    new KeyDumper().run(args)
  }
}