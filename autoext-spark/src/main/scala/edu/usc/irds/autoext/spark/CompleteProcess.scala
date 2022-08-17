package edu.usc.irds.autoext.spark

class CompleteProcess extends SparkJob {

  def run(): Unit ={
    val styleSimilarity = new ContentSimilarityComputer
    styleSimilarity.simFunc = "style"
    styleSimilarity.s3Path = s3Path
    styleSimilarity.sc = sc
    styleSimilarity.run()

    val structureSimilarity = new ContentSimilarityComputer
    structureSimilarity.simFunc = "structure"
    structureSimilarity.s3Path = s3Path
    structureSimilarity.sc = sc
    structureSimilarity.run()

    val similarityCombiner = new SimilarityCombiner
    similarityCombiner.weight = 0.5
    similarityCombiner.in1Path = s"${s3Path}results/structure"
    similarityCombiner.in2Path = s"${s3Path}results/style"
    similarityCombiner.outPath = s"${s3Path}results/combined"
    similarityCombiner.sc = sc
    similarityCombiner.run()

    val sharedNeighborCluster = new SharedNeighborCuster
    sharedNeighborCluster.sharedNeighborThreshold = 0.5
    sharedNeighborCluster.similarityThreshold = 0.7
    sharedNeighborCluster.s3Path = s3Path
    sharedNeighborCluster.sc = sc
    sharedNeighborCluster.run()

    val d3exp = new D3Export
    d3exp.s3Path = s3Path
    d3exp.sc = sc
    d3exp.run()
  }
}

object CompleteProcess {

  def main(args: Array[String]) {
    new CompleteProcess().run(args)
  }

}
