package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.log4j.{Level, Logger}

object Problem2 {
  def main(args:Array[String]){
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
      val inputFile = args(0)
      val k = args(1).toInt
      val conf = new SparkConf().setAppName("Problem2").setMaster("local")
      val sc = new SparkContext(conf)
      
      val input = sc.textFile(inputFile)     
      val edges = input.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 1.0))
      // build a graph by edges with the same attribute
      val graph = Graph.fromEdges(edges, 0.0)
      // initialize the graph
      val initialGraph = graph.mapVertices((id, _) => Set[List[VertexId]]())

      // iterate before the last one iteration
      val res1 = initialGraph.pregel(Set[List[VertexId]](), maxIterations = k-1)(
          vprog = (id, msg, newMsg) => msg ++ (newMsg + List(id)), 
          sendMsg = triplet => { 
            Iterator((triplet.srcId, for  
                // if dstAttr does not contain srcId, plus; otherwise pass
                {idList <- triplet.dstAttr 
                  if (idList.size < k) 
                    if (!idList.contains(triplet.srcId))
                }yield List(triplet.srcId) ++ idList))
          },
          mergeMsg = (a,b) => a ++ b
        )
        
      // last iteration
      val res2 = res1.pregel(Set[List[VertexId]](), 1)(
          vprog = (id, msg, newMsg) => msg ++ (newMsg + List(id)),
          sendMsg = triplet => {            
            Iterator((triplet.srcId, for 
                // if the last id of dstAttr == srcId, plus; otherwise pass
                {idList <- triplet.dstAttr 
                  if (idList.size == k) 
                    if (idList.reverse.head.toInt == triplet.srcId.toInt)
                }yield List(triplet.srcId) ++ idList))
          },
          mergeMsg = (a, b) => a ++ b
        )

      // get the longest path which is the cycle path in each srcId
      // and then flatting the values: 
      // (srcId, Set(List(p,a,t,h))) ==> (srcId, List(p,a,t,h))
      val res3 = res2.vertices.flatMapValues(_.filter(_.size == k+1))
      
      // remove the duplicated srcId from the path and then sort remaining vertices
      val res4 = res3.mapValues(_.drop(1).sorted)

      // remove the duplicated path and output the size of the pathSet which is required answer
      var pathSet = Set[List[VertexId]]()
      for (path <- res4.values.collect){
        pathSet = pathSet + path
      }
      
//        println("maxIteration: "+k+", the number of cycles is: ")
      
      println(pathSet.size)
      
    }  
}