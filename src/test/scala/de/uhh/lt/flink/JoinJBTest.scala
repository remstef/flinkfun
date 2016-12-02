package de.uhh.lt.flink

import org.junit.Test
import de.uhh.lt.flink.BeforeTests._

@Test
class JoinJBTest {
  
  @Test
  def joinJBTest() = {
    
    // expect 1451344
    
    val in = ClassLoader.getSystemClassLoader().getResource("ct-raw").getPath()
    val tmp = s"${_temp_folder}/jb-joined-flink-tmp"
    val out = s"${_temp_folder}/jb-joined-flink"
    JoinJBD.main(Array(
      "-parallelism", "2",
      "-in", in,
      "-out", out,
      "-tmpdir", tmp
      ))
    println(s"saved data in '${out}'.")
  }
  
}