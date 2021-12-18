# Hadoop MapReduce

The repo contains the Hadoop MapReduce demo in Java.

 - Hadoop version: 2.7.5
 - Can run on Windows local directory
 - Need configured HDFS to run on yarn cluster
 - Using Maven.

## Prerequisite

When running the mapreduce job in windows, it may throw an exception:

`java.lang.UnsatisfiedLinkError: org.apache.hadoop.util.NativeCrc32.nativeCompute 
ChunkedSumsByteArray(II[BI[BIILjava/lang/String;JZ)V`

This is because the HDFS is not compatible to the Windows file system. Visit the link bellow, 
follow the steps, and restart the pc, then this error should be fixed.

[Hadoop Fixing UnsatisfiedLinkError](https://sparkbyexamples.com/spark/spark-hadoop-exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io-nativeio-nativeiowindows-access0ljava-lang-stringiz/)

The input/output directory is set to `D:\mapreduce`. To change the path, go to `main/java/mapreduce/utils/SystemPathConstant.java`.

## Intro

Mapreduce is a divide-and-conquer programming concept/model for processing big data on a parallel
distributed cluster. Its implementation can use any programming languages. The most widely used platform 
for MapReduce is <b>Apache HDFS</b>.

The MapReduce contains five steps:
 1. Input Data
 2. <b>Map</b>: <key1, val1> -> <key2, val2>
 3. <b>Shuffle</b>: sort, deduplicate, tag, merge, or other operations on <key2, val2> to <key3, val3>
 4. <b>Reduce</b>: <key3, val3> -> <key4, val4>; reduce step can use multiple reducers
 5. Output

Step 3, 4, and 5 can run in parallel.

The MapReduce concept is difficult to describe without examples, so I will use examples to demo MapReduce.

## MapReduce Examples

<hr />

### Word Count :link:[Link](/src/main/java/mapreduce/word_count)

The input file format is many lines of words separated by single space ' '.
```text
mapreduce local test
mapreduce hdfs run
local run
test run
hdfs test
```

The task is to count the frequency of each word.

 1. Input: text file.
 2. Mapper
    - input <key1, val1>: <line number, String of that line>, like <2, "mapreduce hdfs run">
    - task: split the string by space ' '
    - output <key2, val2>: <single word, 1 (constant)>, like <"mapreduce", 1>, <"hdfs", 1>, <"run", 1>
    
 3. Shuffle: this demo does not need this step yet...
 4. Shuffle
    - input <key2, val2>: from Mapper <key2, val2>
    - task: merge identical word and count their frequency
    - output <key3, val3>: <unique single word, frequency>, like <"mapreduce", 1>, <"hdfs", 2>, <"run", 3>
    
 5. Output <key3, val3> to a text file. Here the file name is `part-r-00000`. The result in the output file is:
```text
hdfs	2
local	2
mapreduce	2
run	3
test	3
```

<hr />

