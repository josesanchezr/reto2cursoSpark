package models

case class FlightsStats(destination: String,
                        morningFlights: Long,
                        afternoonFlights: Long,
                        nightFlights: Long)
