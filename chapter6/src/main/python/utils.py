#!/usr/bin/env python3

import math

EARTH_DIAMETER = 6_371_000 # meters
METER_TO_ANGLE = 180.0 / (EARTH_DIAMETER * math.pi)

def calculateDelta(deltaLatLon):
  return math.sqrt(2 * (1 - math.cos(deltaLatLon)))

def distance(p1, p2):
  deltaLatitude = (p1[0] - p2[0]) * math.pi / 180
  deltaLongitude = (p1[1] - p2[1]) * math.pi / 180
  latitudeIncInMeters = calculateDelta(deltaLatitude)
  longitudeIncInMeters = calculateDelta(deltaLongitude)
  return EARTH_DIAMETER * math.sqrt(
    latitudeIncInMeters * latitudeIncInMeters + longitudeIncInMeters * longitudeIncInMeters)

def move(start, direction, speed, duration):
  angularDistance = speed * duration * METER_TO_ANGLE
  stepSize = math.sqrt(direction[0] ** 2 + direction[1] ** 2)
  directionDelta = tuple(map(lambda x: x / stepSize * angularDistance, direction))
  return tuple(map(sum, zip(start, directionDelta))) + (start[2] + duration, )

