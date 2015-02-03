package org.CMPT732A1;
import java.io.IOException;

//import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.Configuration;

public class LetterCount {
	
	public static class Map extends Mapper<LongWritable, Text,Text, IntWritable>{
		IntWritable One  =  new IntWritable(1);
		Text current_letter =  new Text("abcde");
		@Override
		public void map(LongWritable offset, Text raw_sentence, Context context) 
				throws IOException, InterruptedException{
			String sentence  =  raw_sentence.toString();
			sentence = sentence.replaceAll("[^a-zA-Z]", "");
			char[] letter_array = sentence.toCharArray();
			for (char single_letter : letter_array){
				if (single_letter != ' ')//remove whitespace
				{
					current_letter.set(String.valueOf(single_letter).toLowerCase());;
					context.write(current_letter, One);
				}
				
			}
			
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, LongWritable> {
		LongWritable cnt_sum = new LongWritable(0);
		@Override
		public void reduce(Text letter, Iterable<IntWritable> cnt_ones, Context context) 
				throws IOException, InterruptedException{
			long sum = 0;
			for(IntWritable one : cnt_ones){
				sum += one.get();
			}
			cnt_sum.set(sum); 
			context.write(letter, cnt_sum);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf =  new Configuration();
		Job job = Job.getInstance(conf, " Letter Count");
		job.setJarByClass(LetterCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		// TODO: Compute and stdout the letter frequency
//        Text letter = new Text();
//        LongWritable cnt = new LongWritable();
//        //long den = 0;
//        //long num = 0;
//        Path path_out = new Path(new Path(args[1]), "part-r-00000");
//        SequenceFile.Reader reader  =  new SequenceFile.Reader(conf, SequenceFile.Reader.file(path_out));
//        while(reader.next(letter, cnt)){
//        	System.out.println(letter.toString() + ": " + cnt.get());
//        }
//        reader.close();
	}

}
