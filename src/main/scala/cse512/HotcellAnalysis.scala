package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo.createOrReplaceTempView("pickupInfo")
    //get all points within bound
    spark.udf.register("boundContains",(x: Int, y: Int, z: Int)=> HotcellUtils.boundContains(x, y, z, minX, minY, minZ, maxX, maxY, maxZ))
    val cells = spark.sql("select x,y,z,count(*) as attrVal from pickupInfo where boundContains(x,y,z) group by x,y,z")
    cells.createOrReplaceTempView("cells")
    cells.show()
    val sumVal = cells.select(sum("attrVal")).first().getLong(0).toDouble
    val sumSqr = cells.selectExpr("sum(attrVal * attrVal)").first().getLong(0).toDouble
    val mean = (sumVal/numCells)
    val s = Math.sqrt((sumSqr/numCells) - (mean*mean))

    spark.udf.register("isNeighbor",(x1: Int, y1: Int, z1:Int, x2: Int, y2: Int, z2:Int)=>
      HotcellUtils.isNeighbor(x1, y1, z1, x2, y2, z2)
    )
    spark.udf.register("getNeighbors",(x: Int, y: Int, z:Int)=>
      HotcellUtils.getNeighbors(x, y, z, minX, minY, minZ, maxX, maxY, maxZ)
    )
    val withNeighbor = spark.sql("select x,y,z, attrVal, getNeighbors(x,y,z) as numOfNb from cells ")
    withNeighbor.createOrReplaceTempView("withNeighbor")
    val ifNeighbor = spark.sql("select c1.x as x , c1.y as y, c1.z as z, c1.numOfNb, sum(c2.attrVal) as sumAttr from withNeighbor c1 , withNeighbor c2 WHERE isNeighbor(c1.x,c1.y,c1.z, c2.x,c2.y,c2.z) group by c1.x,c1.y,c1.z,c1.numOfNb").persist()

    ifNeighbor.createOrReplaceTempView("ifNeighbor")
    withNeighbor.show()
    spark.udf.register("GScore", (mean: Double,s: Double,numOfNb: Int, sumAttr: Int, numCells: Int) =>
      HotcellUtils.GScore(mean,s,numOfNb, sumAttr, numCells)
    )
    val withGscore =  spark.sql("select x,y,z,GScore("+ mean + ","+ s +",numOfNb,sumAttr," + numCells+") as Gscore from ifNeighbor")
    withGscore.createOrReplaceTempView("withGscore")

    val results = spark.sql("select x,y,z,GScore from withGscore order by Gscore desc")
    results.show()
    return results.coalesce(1)
  }
}