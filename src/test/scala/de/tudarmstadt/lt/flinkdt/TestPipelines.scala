package de.tudarmstadt.lt.flinkdt

import org.junit.Test
import de.tudarmstadt.lt.flinkdt.pipes.ImpliCtJBT
import java.io.File
import org.junit.rules.TemporaryFolder


class TestPipelines {
  
  val _temp_folder: File = {
	  val f: TemporaryFolder = new TemporaryFolder()
    f.create()
    val t = f.getRoot()
    println("created temporary folder: " + t.getAbsolutePath())
    t
  }
  
  @Test
  def testImpliCtJBT(){
    
    val in = "ct-raw"
    val out = _temp_folder
    
    ImpliCtJBT.main(Array(
        "--dt.io.ct.raw", ClassLoader.getSystemClassLoader().getResource(in).getPath(),
        "--c", "dt.io.ct.raw-fields=\"0,1\" \n dt.jobname=testImpliCtJBT",
        "--dt.io.dir", "file://"+out.getAbsolutePath() + "/1"
    ))
    
    ImpliCtJBT.main(Array(
        "--dt.io.ct.accAB", "file://"+out.getAbsolutePath() + "/1/ct.acc.AB.tsv",
        "--dt.pipeline", "N11Sum, N1dotSum, Ndot1Sum",
        "--c", "dt.io.ct.raw-fields=\"0,1\" \n dt.jobname=testImpliCtJBT",
        "--dt.io.dir", "file://"+out.getAbsolutePath() + "/2"
    ))
    
    println(s"Outputfiles can be found in '${out}'.");
    
  }
  
}