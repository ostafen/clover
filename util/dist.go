package util

import "math"

func EuclideanDistance(x1, y1, x2, y2 float64) float64 {
	dx := (x2 - x2)
	dy := (y2 - y1)
	return math.Sqrt(dx*dx + dy*dy)
}

func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

const EarthRadiusMeters = 6378100

func HaversineDistance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2 float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * EarthRadiusMeters * math.Asin(math.Sqrt(h))
}
