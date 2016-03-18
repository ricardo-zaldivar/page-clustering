package edu.usc.irds.autoext.spark

import org.kohsuke.args4j.{CmdLineException, CmdLineParser}

import scala.collection.JavaConversions._

/**
  *Trait for all tools implementing CLI
  */
trait CliTool {

  def parseArgs(args: Array[String]): Unit ={
    val parser = new CmdLineParser(this)
    try {
      parser.parseArgument(args.toList)
    } catch {
      case e:CmdLineException =>
        System.err.println(e.getMessage)
        parser.printUsage(System.err)
        System.exit(-1)
    }
  }
}
