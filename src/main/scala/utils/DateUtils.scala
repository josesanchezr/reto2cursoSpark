package utils

import java.text.SimpleDateFormat

import scala.util.Try

object DateUtils {
  private val SHORT_FORMAT = "yyyy-mm-dd"
  private val YEAR_FORMAT = "yyyy"
  private val WEEKDAY_FORMAT = "EEEE"

  def getYearFromDateString(dateString: String): Try[String] = {
    val parser = new SimpleDateFormat(SHORT_FORMAT)
    val formatter = new SimpleDateFormat(YEAR_FORMAT)
    Try(formatter.format(parser.parse(dateString)))
  }

  def getDayOfWeekString(dateString: String): Try[String] = {
    val parser = new SimpleDateFormat(SHORT_FORMAT)
    val formatter = new SimpleDateFormat(WEEKDAY_FORMAT)
    Try(formatter.format(parser.parse(dateString)))
  }
}
