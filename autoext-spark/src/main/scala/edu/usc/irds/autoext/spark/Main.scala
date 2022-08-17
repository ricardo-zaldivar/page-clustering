/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.usc.irds.autoext.spark

object Main {

  val cmds = Map[String, (Class[_], String)](
    "help" -> (null, "Prints this help message."),
    "similarity" -> (classOf[ContentSimilarityComputer], "Computes similarity between documents."),
    "sncluster" -> (classOf[SharedNeighborCuster], "Cluster using Shared near neighbor algorithm."),
    "simcombine" -> (classOf[SimilarityCombiner], "Combines two similarity measures on a linear scale."),
    "d3export" -> (classOf[D3Export], "Exports clusters into most popular d3js format for clusters."),
    "comprocess" -> (classOf[CompleteProcess], "Execute the complete process, from extract similarity to build json with clusters")
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
