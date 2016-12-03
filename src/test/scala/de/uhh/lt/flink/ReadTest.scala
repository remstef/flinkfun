package de.uhh.lt.flink

import org.apache.flink.api.scala._

object ReadTest {
  
  def main(args: Array[String]): Unit = {
    
    case class TE(a:String, b:String)
    
    val in = ClassLoader.getSystemClassLoader().getResource("ct-raw").getPath()
    val env = ExecutionEnvironment.getExecutionEnvironment
    val d = env.readCsvFile[TE](in, fieldDelimiter="\t", includedFields=Array(1,2), pojoFields=Array("b", "a"))
    d.first(10).print()  
    
  }
  
}