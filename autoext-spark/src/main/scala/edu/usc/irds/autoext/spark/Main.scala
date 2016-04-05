package edu.usc.irds.autoext.spark

object Main {

  val cmds = Map[String, (Class[_], String)](
    "help" -> (null, "Prints this help message."),
    "partition" -> (classOf[ContentPartitioner], "Partitions Nutch Content based on host names."),
    "keydump" -> (classOf[KeyDumper], "Dumps all the keys of sequence files(s)."),
    "grep" -> (classOf[ContentGrep], "Greps for the records which contains url and content type filters."),
    "merge" -> (classOf[ContentMerge], "Merges (smaller) part files into one large sequence file."),
    "similarity" -> (classOf[ContentSimilarityComputer], "Computes similarity between documents."),
    "sncluster" -> (classOf[SharedNeighborCuster], "Cluster using Shared near neighbor algorithm."),
    "dedup" -> (classOf[DeDuplicator], "Removes duplicate documents (exact url matches)."),
    "d3export" -> (classOf[ContentSimilarityComputer], "Exports clusters into most popular d3js format for clusters.")
  )

  def printAndExit(exitCode:Int = 0, msg:String = "Usage "): Unit ={
    println(msg)
    println("Commands::")
    cmds.foreach({case (cmd,(cls, desc))=> println(String.format("    %-9s  - %s", cmd, desc))})
    System.exit(exitCode)
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      printAndExit(1, "Error: Invalid args")
    } else if (!cmds.contains(args(0)) || args(0).equalsIgnoreCase("help")){
      printAndExit(1)
    } else {
      val method = cmds.get(args(0)).get._1.getDeclaredMethod("main", args.getClass)
      method.invoke(null, args.slice(1, args.length))
    }
  }
}
