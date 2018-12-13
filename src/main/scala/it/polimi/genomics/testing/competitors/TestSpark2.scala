package it.polimi.genomics.testing.competitors

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cctta 7259
  * Created by andreagulino on 16/10/17.
  */
object TestSpark2 {


  def main(args: Array[String]): Unit = {

    var input  = "resources/data.txt"
    var output = "resources/output/"


    val conf = new SparkConf().setAppName("gspm").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile(input)

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

//    val minConfidence = 0.8
//    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
//      println(
//        rule.antecedent.mkString("[", ",", "]")
//          + " => " + rule.consequent .mkString("[", ",", "]")
//          + ", " + rule.confidence)
//    }

  }
}