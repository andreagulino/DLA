package it.polimi.genomics

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File

/**
  * Created by andreagulino on 14/11/17.
  */
object Algorithm3 {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  private final val usage: String = "Usage options: \n"+
    " -minSupport Int \n " +
    " -minLen Int \n"+
    " -maxLen String \n"+
    " -input String \n"+
    " -output String \n"+
    " -sparkMaster String \n"+
    "-minPartitions Int \n"+
    " -advStats true"

  def main(args : Array[String]) {

    println("DLA")

    // Algorithm Parameters
    var minLen  = 2
    var maxLen: Option[Int] = None
    var minSupport = 2
    var sparkMaster = "local[*]"

    // Application Parameters
    var input  = "resources/data.txt"
    var output = "resources/output1/"
    var advStats  = false // count the instances to process at each step?
    var minPartitions = 5


    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)
      } else if ("-minSupport".equals(args(i))) {
        minSupport = args(i + 1).trim.toInt
        println("minSupport: " + minSupport)

      } else if ("-minLen".equals(args(i))) {
        minLen = args(i + 1).trim.toInt
        println("minLen set to: " + minLen)

      }  else if ("-maxLen".equals(args(i))) {
        maxLen = Some(args(i + 1).trim.toInt)
        println("maxLen set to: " + maxLen)

      } else if ("-input".equals(args(i))) {
        input = args(i + 1)
        println("input set to: " + input)

      } else if ("-output".equals(args(i))) {
        output = args(i + 1)
        println("Intermediate results will be counted.")
      } else if ("-sparkMaster".equals(args(i))) {
        sparkMaster = args(i + 1)
        println("Spark Master :"+sparkMaster)
      } else if ("-minPartitions".equals(args(i))) {
        minPartitions = args(i + 1).toInt
        println("Min Partitions :"+minPartitions)
      }
      else if ("-advStats".equals(args(i))) {
        advStats = true
        println("output set to: " + output)
      } else {
        println("Command option is not found ${args(i)}")
        println(usage)
      }
    }

    run(minLen, maxLen, minSupport, input, output, advStats,sparkMaster, minPartitions)

  }


  def run( MIN_LENGTH: Int,  maxLen: Option[Int], MIN_SUPPORT: Int , inputFile: String  , output: String, advStats: Boolean, sparkMaster:String, minPartitions:Int) {


    // Application Setup: delete output folder and setup Spark
    File(output).deleteRecursively()

    val conf = new SparkConf().setAppName("Algorithm_v3").setMaster(sparkMaster)
    val sc = new SparkContext(conf)


    // start measuring time
    val startTime      = System.nanoTime()
    var phaseStartTime = System.nanoTime()

    // Read input data
    val file = sc.textFile(inputFile, minPartitions = minPartitions)


    var candidates : RDD[ ( String, List[(Long, List[Int])] ) ]  = file.mapPartitions( lines => {

      //  partitionMap : substring => List( (seqId, List( positions ) ) )
      val partitionMap = collection.mutable.HashMap[ String, collection.mutable.HashMap[ Long, List[Int] ] ]()

      var numlines = 0

      for( line <- lines ) {

        numlines+=1

        val cols = line.split("\t")
        val id: Long = cols(0).toLong
        val sequence = cols(1)

        if( sequence.length >= MIN_LENGTH ) {

          for( i <- 0 to sequence.length - MIN_LENGTH ) {

            val subseq = sequence.substring(i, i + MIN_LENGTH)

            if( !partitionMap.contains(subseq) ) {
              partitionMap(subseq) = collection.mutable.HashMap[Long, List[Int]](id -> List(i))
            } else {
              val existing =  partitionMap(subseq)

              if( existing.contains(id) ) {
                existing.update(id, i :: existing(id))
              } else {
                existing.update(id, List(i) )
              }

            }

          }
        }

      }


      if( advStats ) {
        println("Partition Report: " + numlines + " lines, " + partitionMap.size + " generated subsequences: ")
        partitionMap.foreach(x => print(x._1+" ") )
      }
      //  RDD[ ( String, List[(Long, List[Int])] ) ]
      partitionMap.map(x => (x._1, x._2.toList)).toIterator

    } )

    /* ITERATIVE STEPS */

    // in result we will store all-length sub-sequences satisfying the minimum support
    var result: Option[RDD[String]] = None

    // variable to store running times and data of each iteration (iteration, entries, time)
    var stats: List[(Long,Long,Long)] = List()

    var converged = false
    var curLength = MIN_LENGTH - 1


    do {

      curLength += 1
      logger.info("Looking for patterns with length: "+curLength)

      // 1.
      val counted: RDD[(String, List[(Long, List[Int])])] = candidates.reduceByKey(
        (el1, el2) =>  el1 ::: el2
      )

      // 2.
      val filtered: RDD[(String, List[(Long, List[Int])])] = counted.filter(_._2.length >= MIN_SUPPORT)

      val elCount = if(advStats) candidates.count() else -1

      // check for convergence
      if( !filtered.isEmpty() && !(maxLen.isDefined && curLength > maxLen.get) ) {

        //filtered.foreach(print(_))


        // measure the time after the reduce / count and add statistics
        val phaseTime = (System.nanoTime() - phaseStartTime) / 1000000.0

        stats = ( (curLength - MIN_LENGTH).toLong, elCount.toLong, phaseTime.toLong) :: stats

        // start a new timee
        phaseStartTime = System.nanoTime()


        // store previous iteration sub-sequences satisfying the minimum support
        val cur_result = filtered.map(x => x._1 + "\t" + x._2.length)
        result = if (result.isEmpty) Some(cur_result) else Some(result.get.union(cur_result))

        // 3.1
        val bysequence: RDD[(Long, List[(String, List[Int])])] = filtered.flatMap(x => {
          for (y <- x._2)
            yield (y._1, List((x._1, y._2)))
        })

        // 3.2
        val grouped: RDD[(Long, List[(String, List[Int])])] = bysequence.reduceByKey(
          (el1, el2) =>  el1 ::: el2
        )

        // 3.3
        candidates = grouped.flatMap(entry => {

          val key = entry._1
          val value = entry._2

          val flatten: Iterable[(String, Int)] =
            for( tuple <- value; el <- tuple._2)
              yield  (tuple._1, el)
          val sorted = flatten.toList.sortBy(_._2)

          val map = collection.mutable.Map[String, List[Int]]()

          // check within each sequence if the next element starts right after the first

          if (sorted.length >= 2) {
            for (i <- 0 to sorted.length - 2) {
              val first  = sorted(i)

              val second = sorted(i + 1)
              val seq = first._1 + second._1.last // new sub-sequence
              map(seq) = if (map.keySet.exists(_ == seq)) first._2 :: map(seq) else List(first._2)

            }
          }


          for (el <- map) yield (el._1, List( (key, el._2) ))

        })

      } else {

        // measure the time after the reduce / count and add statistics
        val phaseTime = (System.nanoTime() - phaseStartTime) / 1000000.0
        stats = ( (curLength - MIN_SUPPORT).toLong, elCount.toLong, phaseTime.toLong) :: stats

        converged = true;
      }

    } while (!converged)

    val stopTime = (System.nanoTime() - startTime) / 1000000.0;
    print("\n \n Elapsed Time: "+stopTime+ " ms. \n \n")
    print(stats.sortBy(_._1))

    // Store final results (repartition to have a single file)
    if(result.isDefined)
      result.get.coalesce(1).saveAsTextFile(output)
    else
      print("No pattern found.")

  }


}