#!/usr/bin/env -S python3 -u

import sys
import math
import time
import random

EARTH_DIAMETER = 6371000
METER_TO_ANGLE = 180.0 / (EARTH_DIAMETER * math.pi)


def usage():
  sys.stderr.write("Usage: %s <num_distinct_tracks>\n" % (sys.argv[0],))
  sys.exit(1)

def move(pos, direction, speed, t):
  step = speed * t * METER_TO_ANGLE
  deltaSize = math.sqrt(direction[0]**2 + direction[1]**2)
  return (pos[0] + direction[0] / deltaSize * step,
          pos[1] + direction[1] / deltaSize * step)


if __name__ == "__main__":
  if len(sys.argv) < 2:
    usage()

  numTracks = int(sys.argv[1])
  nowMs = int(time.time() * 1000)
  positions = [((random.random() - 0.5) * 180, (random.random() - 0.5) * 180) for i in range (0, numTracks)]
  f = [1 + random.random() * 10 for i in range (0, numTracks)]
  while True:
    for track in range(0, numTracks):
      print("user%d\t%f\t%f\t%d" % (track, positions[track][0], positions[track][1], nowMs))
      speed = random.random() * f[track]
      if random.random() < 0.2:
        f[track] = 1 + random.random() * 10
      positions[track] = move(positions[track], (random.random(), random.random()), speed, 60)
    time.sleep(2)
    nowMs += 60000
      

