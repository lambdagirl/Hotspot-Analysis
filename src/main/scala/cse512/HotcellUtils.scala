package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def boundContains(x: Int, y: Int, z:Int, minX: Double, minY: Double, minZ: Double, maxX: Double, maxY:Double, maxZ: Double ): Boolean = {
     x >= minX && x <= maxX && y >= minY && y <= maxY && z >= minZ && z <= maxZ
  }

  def isNeighbor(x1: Int, y1: Int, z1:Int, x2: Int, y2: Int, z2:Int): Boolean = {
    (x1-x2)*(x1-x2) <=1 && (y1-y2)*(y1-y2) <= 1 &&  (z1-z2)*(z1-z2) <= 1
  }

  def getNeighbors(x: Int, y: Int, z:Int): Int = {

    val xEdge = if (x == 1) 1 else 0
    val yEdge = if (y == 1) 1 else 0
    val zEdge = if (z == 1) 1 else 0
    val total = xEdge + yEdge + zEdge

    if (total == 3) {
      return 8
    }
    if (total == 2) {
      return 12
    }
    if (total == 1) {
      return 18
    }
    return 27
  }


  def GScore(mean: Double,s: Double,numOfNb: Int, sumAttr: Int, numCells: Int): Double = {
    val numerator = sumAttr-(mean*numOfNb)
    val denominator = s*Math.sqrt((numCells*numOfNb - numOfNb*numOfNb)/(numCells-1))
    numerator/denominator
  }
}
