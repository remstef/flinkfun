package de.tudarmstadt.lt.flinkdt

/**
 * Created by Steffen Remus.
 */
object JoBim {

  val EMPTY = JoBim("","")

  def fromString(jobimString: String): JoBim = {
    jobimString.split("\t") match {
      case Array(jo, bim, freq_cooc, freq_jo, freq_bim, freq_sig, freq_distinct_jo, freq_distinct_bim, freq_disticnct_sig) => JoBim(jo, bim, freq_cooc.toLong, freq_jo.toLong, freq_bim.toLong, freq_sig.toDouble, freq_distinct_jo.toInt, freq_distinct_bim.toInt, freq_distinct_bim.toDouble)
      case _ => EMPTY
    }
  }

}

case class JoBim (var jo: String,
                  var bim: String,
                  var freq_cooc: Long = 1,
                  var freq_jo: Long = 1,
                  var freq_bim: Long = 1,
                  var freq_sig: Double = 0d,
                  var freq_distinct_jo: Int = 1,
                  var freq_distinct_bim: Int = 1,
                  var freq_distinct_sig: Double = 0d) {

  def flip(): JoBim = copy(
    jo=bim,
    bim=jo,
    freq_jo=freq_bim,
    freq_bim=freq_jo,
    freq_distinct_jo=freq_distinct_bim,
    freq_distinct_bim=freq_distinct_jo)

}
