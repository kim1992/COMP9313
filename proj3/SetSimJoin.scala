package comp9313.proj3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast 
import java.lang.Math.ceil
import org.apache.log4j.{Level, Logger}

object SetSimJoin {
  
  def main(args: Array[String]) {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFolder = args(2)
    val threshold = args(3).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)
    
    // input two files as RDD and store records in the format: 
    // Array[(recordId, Array[record])]
    val input1 = sc.textFile(inputFile1)
    val input2 = sc.textFile(inputFile2)    
    val record1 = input1.map(_.split(" ").map(_.toInt)).map(x => (x(0), x.tail))
    val record2 = input2.map(_.split(" ").map(_.toInt)).map(x => (x(0), x.tail))
    
//    record1.persist()
//    record2.persist()
    
    // sort the records according to token frequency
    val freqOrder1 = sc.broadcast(record1.map(_._2).flatMap {x => x}.map(x => (x,1)).reduceByKey(_+_).collectAsMap)
    val freqOrder2 = sc.broadcast(record2.map(_._2).flatMap {x => x}.map(x => (x,1)).reduceByKey(_+_).collectAsMap)
   
    val orderedRecord1 = record1.map(x=>(x._1, x._2.sortWith(_.toInt < _.toInt).sortBy(e=>freqOrder1.value(e))))
    val orderedRecord2 = record2.map(x=>(x._1, x._2.sortWith(_.toInt < _.toInt).sortBy(e=>freqOrder2.value(e))))
    
    // compute prefix of each record as the formula: p = |r| - ceil(|r| * t) + 1
    // and then output prefix records: 
    // Array[(prefix(i), Array[id + record])]
    val prefixArray1 = orderedRecord1.flatMap{x=>
            val id = x._1
            val recordArray = x._2                                
            val p = recordArray.length - ceil(recordArray.length * threshold).toInt + 1
            val prefixList = recordArray.take(p)
            for (i<- 0 to prefixList.length-1) yield(prefixList(i), Array(id)++recordArray)                                
     }
    
    val prefixArray2 = orderedRecord2.flatMap{x=>
            val id = x._1
            val recordArray = x._2                                
            val p = recordArray.length - ceil(recordArray.length * threshold).toInt + 1
            val prefixList = recordArray.take(p)
            for (i<- 0 to prefixList.length-1) yield(prefixList(i), Array(id)++recordArray)                                
     }
    
    // join prefixArray1 and prefixArray2
    val joinRecord = prefixArray1.join(prefixArray2)
    
    // compute the similarity between prefixArray1 and prefixArray2 
    // Jaccard similarity formula: intersect(a,b) / union(a,b)
    // and then round the result to 6 decimal places using BigDecimal
    val similarity = joinRecord.map{x =>
       val id1 = x._2._1(0)
       val id2 = x._2._2(0)
       val record1 = x._2._1.drop(1)
       val record2 = x._2._2.drop(1)
       val unionLength = record1.union(record2).distinct.length.toDouble
       val intersectLength = record1.intersect(record2).length.toDouble
       val jaccard = intersectLength / unionLength
       val sim = BigDecimal(jaccard).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
       ((id1, id2),sim)
    }
    
    // filter the result that meets the requirement of threshold
    // and then sort the result in ascending order by the first record and then the second.
    val result = similarity.filter(x => x._2 >= threshold)
    .distinct.sortBy(x=>(x._1._1,x._1._2))
    
    // convert the result format to update the output
    val output = result.map(x=> x._1+"\t"+x._2)
    
//    output.foreach(println)
    output.saveAsTextFile(outputFolder)

    
    
    
  }
  
}