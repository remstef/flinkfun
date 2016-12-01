package de.uhh.lt.flink

import java.io.File
import org.junit.BeforeClass
import org.junit.rules.TemporaryFolder

object BeforeTests {
  
  val _temp_folder: File = {
      val f: TemporaryFolder = new TemporaryFolder()
      f.create();
      val t = f.getRoot()
      println(s"created temporary folder: ${t.getAbsolutePath}")
      t
  }
  
}