#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
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

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

lines = lines.flatMap(lambda line: parseLine(line)).reduceByKey(lambda a, b: a + b)

#TODO

leagueIds = sc.textFile(sys.argv[2], 1)
leagueIds = leagueIds.map(lambda id: (id,0))

lines = lines.join(leagueIds).map(lambda edge: (edge[0], edge[1][0]))

#print(lines.collect)
lines = lines.collect()

lines = sorted(lines, key=lambda line: line[1])

lines = [(line[0], i) for i, line in enumerate(lines)]

lines = sorted(lines, key=lambda line: line[0])

#write results to output file. Foramt for each line: (key + \t + value +"\n")
output = open(sys.argv[3], "w")
for line in lines:
    #print(line)
    output.write(f'{line[0]}\t{line[1]}\n')

# for id in leagueIds.take(10):
#     output.write(f'ID: {id}\n')

sc.stop()
