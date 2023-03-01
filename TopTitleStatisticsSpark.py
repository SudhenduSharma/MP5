#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

rdd = sc.textFile(sys.argv[1], 1)
#tiles_df = rdd.collect()
#print(tiles_df)
second_column_rdd = rdd.map(lambda line: line.split("\t")[1])
int_rdd = second_column_rdd.map(lambda x: int(x))
print("############")
print("############")
print("############")

# calculate mean
mean = int(int_rdd.mean())

# calculate sum
total = int_rdd.sum()

# calculate minimum
minimum = int_rdd.min()

# calculate maximum
maximum = int_rdd.max()

# calculate variance
variance = int(int_rdd.map(lambda x: (x - mean) ** 2).mean())


# print statistics
print("Mean: {:.2f}".format(mean))
print("Sum: {:.2f}".format(total))
print("Minimum: {:.2f}".format(minimum))
print("Maximum: {:.2f}".format(maximum))
print("Variance: {:.2f}".format(variance))

# stop the Spark context
print("############")
print("############")
print("############")

#TODO


outputFile = open(sys.argv[2], "w")
outputFile.write(f'Mean\t{mean}\n')
outputFile.write(f'Sum\t{total}\n')
outputFile.write(f'Min\t{minimum}\n')
outputFile.write(f'Max\t{maximum}\n')
outputFile.write(f'Var\t{variance}\n')

'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''

sc.stop()
