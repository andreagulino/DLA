package it.polimi.genomics.testing.competitors

import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * cctta 7259
  * Created by andreagulino on 16/10/17.
  */
object TestSpark {


  def main(args: Array[String]): Unit = {

    var input  = "resources/data.txt"
    var output = "resources/output/"


    val conf = new SparkConf().setAppName("gspm").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val file: RDD[String] = sc.textFile(input)

    val inputdata: RDD[Array[Array[Char]]] = file.map(x => {
      val a: ArrayBuffer[Array[Char]] = ArrayBuffer()
      for(c <- x.toArray) {
        a += Array(c)
      }
      a.toArray
    })

    val sequences = sc.parallelize(Seq(
      Array(Array("a"),Array("d"),Array("c"), Array("b") ),
      Array(Array("b"),Array("c"), Array("d")),
      Array(Array("c"),Array("b"), Array("d")),
      Array(Array("d"),Array("b"), Array("x"))
    ), 2).cache()
    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.1)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(inputdata)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
  }
}