// Find K Means of Loudacre device status locations
// 
// Input data: file(s) with device status data (delimited by '|')
// including latitude (13th field) and longitude (14th field) of device locations 
// (lat,lon of 0,0 indicates unknown location)

import scala.math.pow

// The squared distances between two points
def distanceSquared(p1: (Double,Double), p2: (Double,Double)) = { 
  pow(p1._1 - p2._1,2) + pow(p1._2 - p2._2,2 )
}

// The sum of two points
def addPoints(p1: (Double,Double), p2: (Double,Double)) = {
  (p1._1 + p2._1, p1._2 + p2._2)
}

// for a point p and an array of points, return the index in the array of the point closest to p
def closestPoint(p: (Double,Double), points: Array[(Double,Double)]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until points.length) {
      val dist = distanceSquared(p,points(i))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
}

// The device status data file(s)
val filename = "/loudacre/devicestatus_etl/*"

// K is the number of means (center points of clusters) to find
val K = 5

// ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
val convergeDist = .1
    
// Parse the device status data file into pairs
val fileRdd = sc.textFile(filename)
val data = fileRdd.map(line => line.split(',')).map(pair => (pair(3).toDouble, pair(4).toDouble)).filter(point => !((point._1 == 0) && (point._2 == 0))).
      persist()

//start with K randomly selected points from the dataset
var kPoints = data.takeSample(false, K, 42)

println("K center points: " )
kPoints.foreach(println)

// loop until the total distance between one iteration's points and the next is less than the convergence distance specified
var tempDist = Double.PositiveInfinity
while (tempDist > convergeDist) {

    // for each point, find the index of the closest kpoint.  map to (index, (point,1))
    val closestKpoint = data.map(point => (closestPoint(point, kPoints), (point, 1)))
    
        
    // For each key (k-point index), reduce by adding the coordinates and number of points
    val rp = closestKpoint.reduceByKey{case ((point1,n1),(point2,n2)) => (addPoints(point1,point2),n1+n2) }
    
    // For each key (k-point index), find a new point by calculating the average of each closest point
    val newp = rp.map{case (i,(point,n)) => (i,(point._1/n,point._2/n))}.collectAsMap()
    
    // calculate the total of the distance between the current points and new points
    tempDist = 0.0
    for (i <- 0 until K) {
     	 tempDist += distanceSquared(kPoints(i),newp(i))
    }
    
    // Copy the new points to the kPoints array for the next iteration
    for (i <- 0 until K) {
      	kPoints(i) = newp(i)
    }
}
   
// Display the final center points        
println("Final center points: ")
kPoints.foreach(println)
