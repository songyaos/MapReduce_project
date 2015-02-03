package org.CMPT732A1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Read Worldwide temperature variation, and calculate 
 * the yearly average temp variation on all location
 * @version 1.1
 * @author siyongzhu
 *
 */
public class TemperatureVariations {

	
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>{
		
		
		private final String TMAX = "TMAX";
		private final String TMIN = "TMIN";

		private String date = null;
		private String station = null;
		private float max = 0;
		private float min = 0;
		private float diff = 0;
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException ,InterruptedException {
			String line = value.toString();
			String[] params = line.split(",");
				if(params[2].equals(TMAX)){
					max = Float.parseFloat(params[3]);
					date = params[1];
					station = params[0];
				}else if ((params[2].equals(TMIN))&&(params[1].equals(date))&&(params[0].equals(station))){
					min = Float.parseFloat(params[3]);
					diff = (max - min)/10;
					context.write(new Text(date), new FloatWritable(diff));
				}
			
		}
		
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		
		public void reduce (Text date, Iterable<FloatWritable> temps, Context context)
							throws IOException, InterruptedException{
			double sum = 0;
			int total = 0;
			float avg = 0;
			for(FloatWritable temp : temps){
				total++;
				sum += temp.get();
			}
			avg = (float)sum/total;
			context.write(date, new FloatWritable(avg));
			
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		final String OUTPUT_PATH = "output";
		final String OUTPUT_FILE = "part-r-00000";
		Configuration conf = new Configuration();
		
		
		ArrayList<String> dates = new ArrayList<String>();
		ArrayList<Float> tempDiff = new ArrayList<Float>();
		
		Path output = new Path(OUTPUT_PATH,OUTPUT_FILE);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "tempvar");
		
		job.setJarByClass(TemperatureVariations.class);
		
		// Set Map output type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);		
		
		//set reducer output type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		//Set output file format to sequence file output
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		SequenceFile.Reader reader = new SequenceFile.Reader(job.getConfiguration(),
												SequenceFile.Reader.file(output));
		
		Text date = new Text();
		FloatWritable diff = new FloatWritable();
		
		while(reader.next(date, diff)){
			dates.add(date.toString());
			tempDiff.add(diff.get());
		}
		reader.close();
		
		for (int i=0; i<dates.size(); i++){
			System.out.println(dates.get(i) + "\t" + tempDiff.get(i));
		}
		
		
	}
}
