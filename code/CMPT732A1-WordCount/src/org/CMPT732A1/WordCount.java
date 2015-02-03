package org.CMPT732A1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;

public class WordCount {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    	private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
    	@Override
        public void map(LongWritable key,  Text value, Context context)
               throws IOException, InterruptedException {
            // Write me
    		String sentence = value.toString();
    		sentence  = sentence.replaceAll("[^a-zA-Z ]", "");
    		StringTokenizer itr = new StringTokenizer(sentence);
    		while (itr.hasMoreTokens()) {
    	        word.set(itr.nextToken());
    	        context.write(word, one);
    	      }
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	private IntWritable result = new IntWritable();
    	@Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // Write me
    		int sum = 0;
    	      for (IntWritable val : values) {
    	        sum += val.get();
    	      }
    	      result.set(sum);
    	      context.write(key, result);
    	    }
    }
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "word count");
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //long size = (long) (Math.pow(2,64)/2);
        long startTime = 0;
        long size = 134217728;
        long endTime = 0;
        long totalTime = 0;
        String formated_time = null;
    	startTime = System.currentTimeMillis();
    	job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", size/2);
        job.waitForCompletion(true);
        endTime = System.currentTimeMillis();
        totalTime = endTime - startTime;
        
        formated_time = String.format("%d min, %d sec", 
        	    TimeUnit.MILLISECONDS.toMinutes(totalTime),
        	    TimeUnit.MILLISECONDS.toSeconds(totalTime) - 
        	    TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalTime))
        	);
        System.out.println("total time in milli seconds: " + totalTime + " (ms)");
        System.out.println("total time in minutes: " + formated_time);
    }
}

