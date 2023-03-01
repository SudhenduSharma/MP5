#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
import re
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]



with open(stopWordsPath) as f:
  #TODO Done
  stopwords = set(f.read().splitlines())

with open(delimitersPath) as f:
  #TODO Done
  f = open(delimitersPath, "r")
  delimiters = f.read().strip()

# create regular expression pattern that matches any delimiter
delimiter_pattern = re.compile("[" + re.escape(delimiters) + "]")



conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

#Read a text file from HDFS, a local file system (available on all nodes), or any Hadoop-supported file system URI, and return it as an RDD of Strings. The text files must be encoded as UTF-8.
line = sc.textFile(sys.argv[3], 1)

# tokenize RDD using delimiters
tokens_rdd = line.flatMap(lambda line: delimiter_pattern.split(line))

new_line_removed_rdd = tokens_rdd.filter(lambda token: token.rstrip('\n'))

# remove stop words from tokens and strip space
filtered_tokens_rdd = new_line_removed_rdd.filter(lambda token: token.lower() not in stopwords)

# convert filtered tokens to lower case and remove leading/trailing whitespace characters
lowercase_tokens_rdd = filtered_tokens_rdd.map(lambda token: re.sub(r"\s+", "", token.lower().strip()))

# count the occurrences of each word
word_count_rdd = lowercase_tokens_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

ordered_rdd = word_count_rdd.takeOrdered(10, key=lambda x: -x[1])
sorted_rdd = sorted(ordered_rdd,key=lambda i: i[0])

# print the tokens
#print(word_count_rdd.collect())

#write results to output file. Foramt for each line: (line +"\n")
outputFile = open(sys.argv[4],"w")
for e in sorted_rdd:
  if e != ' ' :
    outputFile.write(f'{e[0]}\t{e[1]}\n')

outputFile.close()
#TODO Done
sc.stop()
