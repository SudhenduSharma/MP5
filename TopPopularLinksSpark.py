#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext


def parseLine(line):
    line = line.rstrip('\n').split(': ')
    source = line[0]
    destinations = line[1].split()
    res = [(source, 0)]
    #not like MP4,
    # a : a
    #In this case, a is NOT an orphan page in MP5. Sorry for any inconvenience for that.
    for d in destinations:
        res.append((d,1))
    return res


conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

reduced_rdd = lines.flatMap(lambda line: parseLine(line)).reduceByKey(lambda a, b: a + b)

ordered_rdd = reduced_rdd.takeOrdered(10, lambda line: -line[1])

sorted_rdd = sorted(ordered_rdd, key=lambda val: val[0])

#TODO

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

for res in sorted_rdd:
  output.write(f'{res[0]}\t{res[1]}\n')

sc.stop()
