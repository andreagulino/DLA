package it.polimi.genomics.testing

import java.io.{File, PrintWriter}
import java.util.Random

import org.slf4j.LoggerFactory

object DataGenerator {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  private final val usage: String = "Usage options: \n"+
    " -lines Int \n " +
    " -minLen Int \n"+
    " -maxLen String \n"+
  " -alphabetSize Int(max 58) \n"

  val rand = new Random(System.currentTimeMillis())

  def main(args: Array[String]): Unit = {

    var lines = 200000
    var minLen = 10
    var maxLen = 300
    var alphabetSize = 4
    var max_alphabet = 'A' to 'z'

    for (i <- 0 until args.length if (i % 2 == 0)) {
      if ("-h".equals(args(i)) || "-help".equals(args(i))) {
        println(usage)
        System.exit(0)
      } else if ("-minLen".equals(args(i))) {
        minLen = args(i + 1).trim.toInt
        println("minLen set to: " + minLen)

      } else if ("-maxLen".equals(args(i))) {
        maxLen = args(i + 1).trim.toInt
        println("maxLen set to: " + maxLen)

      } else if ("-lines".equals(args(i))) {
        lines = args(i + 1).trim.toInt
        println("lines set to: " + lines)

      } else if ("-alphabetSize".equals(args(i))) {
        val as = args(i + 1).trim.toInt
        if (as > max_alphabet.length)
          println("WARNING: alphabetSize must be <= 5, using alphabetSize = " + alphabetSize)
        else
          alphabetSize = as

        println("alphabetSize set to: " + alphabetSize)

      } else {
        logger.warn(s"Command option is not found ${args(i)}")
        logger.info(usage)
      }

    }

    generate(lines, minLen, maxLen, max_alphabet.slice(0, alphabetSize).toList)
    print("Generated.")
  }

  def generate(lines: Int, min_len: Int, max_len: Int, alphabet: List[Char]) = {

    val diff = max_len - min_len

    val file    = new File("resources/data.txt")
    print(file.getAbsolutePath)
    val file1  = new File("data_mgfsm.txt")  // format used by MG-FSM

    val writer  = new PrintWriter(file)
    val writer1 = new PrintWriter(file1)

    for( i <- 1 to lines) {

      var seq = ""

      for (i <- 1 to min_len + rand.nextInt(diff)) {
        seq += alphabet(rand.nextInt(4))
      }

      writer.write(i+"\t"+seq+"\n")
      writer1.write(i+" "+seq.replace("", " ").trim()+"\n")
    }


    writer.close()
    writer1.close()

  }
}
