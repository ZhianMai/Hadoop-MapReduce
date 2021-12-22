# Hadoop MapReduce

<p align="center">
  <img src="https://github.com/ZhianMai/ZhianMai/blob/main/img/mapreduce.jpg" width="492" height="180" />
</p>

The repo contains the Hadoop MapReduce demo in Java.

 - Hadoop version: 2.7.5
 - Can run on Windows local directory
 - Need configured HDFS to run on yarn cluster
 - Using Maven.

## Prerequisite

When running the mapreduce job in windows, it may throw an exception:

`java.lang.UnsatisfiedLinkError: org.apache.hadoop.util.NativeCrc32.nativeCompute ChunkedSumsByteArray(II[BI[BIILjava/lang/String;JZ)V`

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

<br />

## MapReduce Examples

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
    - Input <key1, val1>: <line number, String of that line>, like <2, "mapreduce hdfs run">
    - Task: split the string by space ' '
    - Output <key2, val2>: <single word, 1 (constant)>, like <"mapreduce", 1>, <"hdfs", 1>, <"run", 1>
    
 3. Shuffle: merge the pair with same key, like <"mapreduce", [1,1,1]>
 4. Reducer
    - Input <key2, val2>: from Shuffle <key2, valList>
    - Task: merge identical word and count their frequency
    - Output <key3, val3>: <unique single word, frequency>, like <"mapreduce", 1>, <"hdfs", 2>, <"run", 3>
    
 5. Output <key3, val3> to a text file. Here the file name is `part-r-00000`. The result in the output file is:
```text
hdfs	2
local	2
mapreduce	2
run	3
test	3
```

<hr />

### Partition on Gender :link:[Link](/src/main/java/mapreduce/gender_partition)

The first step is to get some mock data from [here](https://www.mockaroo.com/). The data schema in the csv file is:
```text
id  first_name  last_name   email   gender  ip_address  languages
```

A snippet of the data:
```text
1,Tally,Tanner,ttanner0@php.net,M,31.37.234.117,Malay
2,Elke,Cooksey,ecooksey1@imdb.com,F,253.198.7.212,Guaran??
3,Stafani,Gilhool,sgilhool2@imdb.com,F,134.110.57.112,Persian
4,Gerek,Silliman,gsilliman3@yandex.ru,M,149.247.121.13,Telugu
```
Each line is an entity, and each entry in the entity is separated by a comma.

The task is to build a partitioner that can partition entries based on the gender, and feed into two reducers, say,
"M" goes to reducer 1, and "F" goes to reducer 2.

To simplify the task, there is no logical task in the mapper and reducer: they just output the "same" <key, val>.

1. Input: .csv text file.
2. Mapper
    - Input <key1, val1>: <line number, String of that line>, like <1, "1,...,...,...,M,...,...">
    - Task: none
    - Output <key2, val2>: <val1, NullWritable as a placeholder>

3. Shuffle: partition the data based on gender.
    - Input <key2, val2>: <entity, NullWritable>
    - Read the "gender" entry from the entity
    - Assign to a specific reducer based on "M" or "F"
4. Reducer
    - Input <key2, val2>: from Shuffle <key2, NullWritable>
    - Task: none
    - Output <key2>

5. Output <key2> to two text files. Here the file names are `part-r-00000` and `part-r-00001`. The `part-r-00000` 
only contains "F", while the `part-r-00001` only contains "M".
   
Caution: the data for partition must be included in the key that passing to the partitioner.


