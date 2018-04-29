package com.cloudtv.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	 
private final IntWritable ONE = new IntWritable(1);
private Text word = new Text();

public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

String[] words = value.toString().split(" ");
for (String wordStr : words) {
    word.set(wordStr);
    context.write(word, ONE);
}
}

}
