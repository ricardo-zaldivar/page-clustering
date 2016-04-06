package edu.usc.irds.autoext.spark

import org.kohsuke.args4j.{CmdLineParser, Option}

/**
  * Trait for SparkJobs which have Inputs and outputs
  */
trait IOSparkJob extends SparkJob {

  @Option(name = "-in", forbids = Array("-list"),
    usage = "path to a file/folder having input data")
  var inputPath: String = null

  @Option(name = "-list", forbids=Array("-in"),
    usage = "path to a file which contains many input paths (one path per line).")
  var listFilePath: String = null

  @Option(name = "-out", required = true, usage = "Path to file/folder where the output shall be stored")
  var outPath: String = null

  @Option(name = "-locallist", forbids = Array("-in"), depends = Array("-list"),
  usage = "When this flag is set the -list is forced to treat as local file." +
    " By default the list is read from distributed filesystem when applicable")
  var localList: Boolean = false

  override def parseArgs(args:Array[String]): Unit ={
    super.parseArgs(args)
    if (inputPath == null && listFilePath == null) {
      System.err.println("Either -in or -list is required.")
      new CmdLineParser(this).printUsage(System.err)
      System.exit(1)
    }
  }

  /**
    * Gets input paths to this io job
    * @return paths to job
    */
  def getInputPaths(): Array[String] ={
    if (inputPath != null) {
      Array(inputPath)
    } else if (listFilePath != null) {
      val lines =
      if (localList) {
        val src = scala.io.Source.fromFile(listFilePath)
        try src.getLines().toArray finally src.close()
      } else {
        sc.textFile(listFilePath).collect()
      }
      lines.map(_.trim).filter(l => !l.startsWith("#") && !l.isEmpty)
    } else {
      throw new RuntimeException("No input specified")
    }
  }

}

