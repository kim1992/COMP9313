package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

object Problem1 {
    def main(args:Array[String]){
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      val inputFile = args(0)
      val outputFolder = args(1)
      val conf = new SparkConf().setAppName("Problem1").setMaster("local")
      val sc = new SparkContext(conf)
      
      val input = sc.textFile(inputFile)
      val lines = input.map(_.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").filter(_.length > 0)
                         .map(_.toLowerCase).filter(x => x.charAt(0)>='a' && x.charAt(0)<='z')).collect    
                   
      // pairs contains pairs of each line 
      // <term1, (term2, 1)>                         
      val pairs = for { k <- 0 until lines.length
         i <- 0 until lines(k).length
         val term1 = lines(k)(i)
         j <- i+1 until lines(k).length
         val term2 = lines(k)(j)}
      yield (term1, (term2, 1))
         
      // convert pairs into RDD type
      val pairInRdd = sc.parallelize(pairs)
      // totalTerm: <term1, sum(term1)>
      val totalTerm = pairInRdd.map(x => (x._1, x._2._2)).reduceByKey(_+_)
      // totalPairs: <term1, (term2, sum(pair))>
      val totalPairs = pairInRdd.groupByKey.flatMapValues(_.groupBy(_._1)
               .mapValues(_.unzip._2.sum))
      // pairAndTerms: <term1, ((term2, sum(pair), sum(term1))>
      val pairsAndTerms = totalPairs join totalTerm
      // compute relative frequency by sum(pair) / sum(term1)
      val relativeFreq = pairsAndTerms.map(x => (x._1, x._2._1._1, x._2._1._2.toDouble / x._2._2)).collect
      
      // sort the array by term1 in ascending order, relative frequency in descending order, term2 in ascending order
      // result: <term1, term2, frequency>
      val result = relativeFreq.sortBy(x => (x._1, -x._3, x._2))
       
      // update the output format in result
      val output = sc.parallelize(result.map(x => x._1+" "+x._2+" "+x._3))
      
//      output.foreach(println)
      
      // save the output into outputFolder
      output.saveAsTextFile(outputFolder)
                  
    }
}