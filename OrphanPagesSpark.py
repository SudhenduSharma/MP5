#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

#TODO
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


conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

rdd = sc.textFile(sys.argv[1], 1)

#TODO
emitted_rdd = rdd.flatMap(lambda line: parseLine(line)).reduceByKey(lambda a, b: a + b).filter(lambda line: line[1] == 0)
result_rdd = emitted_rdd.takeOrdered(emitted_rdd.count())

output = open(sys.argv[2], "w")

#TODO
#write results to output file. Foramt for each line: (line + "\n")
for line in result_rdd:
    output.write(f'{line[0]}\n')

sc.stop()
