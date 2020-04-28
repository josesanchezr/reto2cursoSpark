package models

case class CancelledFlight(number: String,
                           origin: String,
                           destination: String,
                           cancelled: Long,
                           cause: String)

// TODO Calculating causes like List[(String, Int)]
