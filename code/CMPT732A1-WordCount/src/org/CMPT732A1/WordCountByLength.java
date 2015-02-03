package org.CMPT732A1;

import java.io.IOException;
import java.util.StringTokenizer;
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
import org.apache.hadoop.conf.Configuration;

public class WordCountByLength 
{
	public static class Map extends Mapper<LongWritable, Text,Text, IntWritable>
	{//(offset, "cat dog tiger animal fabulous", (3,1)(3,1)(5,1)(6,1)(8,1))
		IntWritable one = new IntWritable(1);
		Text word =  new Text("");
		@Override
		public void map(LongWritable offset, Text raw_sentence, Context context)
				throws IOException, InterruptedException
			{
						String sentence = raw_sentence.toString();
			    		sentence  = sentence.replaceAll("[^a-zA-Z ]", " ");
			    		StringTokenizer word_itr =  new StringTokenizer(sentence);
			    		while(word_itr.hasMoreTokens())
			    		{
			    			String new_word = word_itr.nextToken();
			    			word.set(new_word.toLowerCase());
			    			context.write(word, one);
			    		}
    		}
	}	 
	public static class Reduce extends Reducer<Text, IntWritable, IntWritable,IntWritable>
	{		//(cat,[1,1,1,1,1,1,1] (tiger,[1,1,1,1,1]) ......)
		int[] wordlen_table= new int[100];
		//int cnt_sum = 0; 
		//LongWritable result = new LongWritable();
		@Override
		public void reduce(Text word, Iterable<IntWritable> cnt_ones, Context context) 
				throws IOException, InterruptedException
		{
			wordlen_table[word.toString().length()] += 1;
			for (int i=0;i<100;i++){ 
				context.write(new IntWritable(i),new IntWritable(wordlen_table[i]) );
			}
		}		
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf =  new Configuration();
		Job job =  Job.getInstance(conf,"Word Count by Length");
		job.setJarByClass(WordCountByLength.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
	}

}
