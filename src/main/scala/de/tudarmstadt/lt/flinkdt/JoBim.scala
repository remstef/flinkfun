package de.tudarmstadt.lt.flinkdt

/**
 * Created by Steffen Remus.
 */
case class JoBim (jo: String,
                  bim: String,
                  freq_cooc: Int = 1,
                  freq_jo: Int = 1,
                  freq_bim: Int = 1,
                  freq_sig: Double = 0d,
                  freq_distinct_jo: Int = 1,
                  freq_distinct_bim: Int = 1,
                  freq_distinct_sig: Double = 0d) {

  def flip(): JoBim = copy(
    jo=bim,
    bim=jo,
    freq_jo=freq_bim,
    freq_bim=freq_jo,
    freq_distinct_jo=freq_distinct_bim,
    freq_distinct_bim=freq_distinct_jo)

}
