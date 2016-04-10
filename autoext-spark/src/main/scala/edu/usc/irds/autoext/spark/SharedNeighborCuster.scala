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

import java.util

import edu.usc.irds.autoext.cluster.SharedNeighborClusterer._
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.Option
import scala.collection.JavaConversions._

/**
  * Shared Near Neighbor Clustering implemented using GraphX on spark
  *
  * @author Thamme Gowda N.
  */
class SharedNeighborCuster extends IOSparkJob {

  @Option(name = "-sim", aliases = Array("--similarityThreshold"),
    usage = "if two items have similarity above this value," +
    " then they will be treated as neighbors. Range[0.0, 1.0]")
  var similarityThreshold: Double = 0.7

  @Option(name = "-share", aliases = Array("--sharingThreshold"),
    usage = "if the percent of similar neighbors in clusters exceeds this value," +
    " then those clusters will be collapsed/merged into same cluster. Range:[0.0, 1.0]")
  var sharedNeighborThreshold: Double = 0.8

  def run(): Unit ={

    val similarityThreshold = this.similarityThreshold //Local variable serialization
    val sharedNeighborThreshold = this.sharedNeighborThreshold

    //STEP : Input set of similarity matrix
    var entryRDD:RDD[MatrixEntry] = sc.union(
      getInputPaths().map(sc.objectFile[MatrixEntry](_)))

    //STEP : Initial set of neighbors
    entryRDD = entryRDD.filter(e => e.value >= similarityThreshold).cache()

    //Step : Initial edges
    var edges = entryRDD.filter(e => e.i < e.j)  // only one edge out of (1,2) and (2, 1)
      .map(e => {Edge(e.i, e.j, e.value)})

    //Step :  Initial set of Vertices
    var vertices = entryRDD
      .map(t => (t.i, (t.j, t.value)))
      .groupByKey()
      .map({case (vId, ns) =>
        val neighbors = new util.BitSet()
        neighbors.set(vId.toInt, true)
        ns.foreach({ case(nId, v) => neighbors.set(nId.toInt, true)}) //FIXME: converting long to int here, possible overflow
        (vId, neighbors)
      })

    var graph:Graph[util.BitSet, Double] = null
    var hasMoreIteration = true
    var iterations:Int = 0

    while (hasMoreIteration) {
      iterations += 1
      //Step : Initial Graph
      graph =  Graph(vertices.distinct(), edges.distinct()).cache()
      println(s" Before Iteration $iterations :: Num vertices=${graph.vertices.count()}," +
        s" Num Edges=${graph.edges.count()}")

      //Step : collapse similar clusters
      val replacementRdd = graph.triplets
        .filter(et => areClustersSimilar(et.srcAttr, et.dstAttr, sharedNeighborThreshold))
        .map(et => (Math.max(et.srcId, et.dstId), Math.min(et.srcId, et.dstId)))

      val replacements = new util.TreeMap[Long, Long](replacementRdd.collectAsMap())
      // resolve the transitive replacements {2=1, 3=2} => {2=1, 3=1}
      for (k <- replacements.keySet()) {
      var key = replacements.get(k)        //TODO: distributed computation if possible
        while (replacements.containsKey(key)){
          key = replacements.get(key)
        }
        replacements.put(k, key)
      }

      println(s"Number of clusters collapsed :: ${replacements.size()}")
      //Step  : Decision : Can there be next Iteration?
      hasMoreIteration = !replacements.isEmpty
      if (hasMoreIteration){
        //Step : Update : Finding vertices for next iteration
        vertices = graph.vertices
          .filter(v=> !replacements.containsKey(v._1)) // un replaced vertices remains
          .map({case (vId, bs) =>
          for ((k,v) <- replacements) {
            if (v != vId && bs.get(k.toInt)) {  // k'th cluster is replaced by v'th cluster
              bs.set(k.toInt, false)  //unset k'th bit and set v'th bit
              bs.set(v.toInt, true)
            }
          }
          (vId, bs)
        })

        //Step 2 : Update : Finding updated edges for the next iteration
        edges = graph.edges.map( e => {
          if (replacements.containsKey(e.srcId) || replacements.contains(e.dstId)) {
            //affected edge
            Edge(replacements.getOrElse(e.srcId, e.srcId),
              replacements.getOrElse(e.dstId, e.dstId), e.attr)
          } else { // un affected
            e
          }
        }).filter(e => e.srcId != e.dstId)
      }
    }

    val clusters = graph.vertices.map({case (id, neighbors) =>

      //To iterate over the true bits in a BitSet, use the following loop:
      var i = neighbors.nextSetBit(0)
      var list = new scala.collection.mutable.ArrayBuffer[Long]()
      while(i >= 0 && i <= Integer.MAX_VALUE) {
        list += i
        i = neighbors.nextSetBit(i+1)
      }
      s"$id,${list.size}," + list.mkString(",")
    }).cache()

    clusters.saveAsTextFile(outPath)
    println(s"Total Clusters = ${clusters.count()}");
    sc.stop()
  }
}


object SharedNeighborCuster {

  def main(args: Array[String]) {
    new SharedNeighborCuster().run(args)
  }
}
