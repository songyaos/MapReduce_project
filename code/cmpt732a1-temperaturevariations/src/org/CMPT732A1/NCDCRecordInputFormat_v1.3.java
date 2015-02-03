package org.CMPT732A1;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class NCDCRecordInputFormat extends TextInputFormat {

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
		return new NCDCRecordReader();
		
	}
	
	public class NCDCRecordReader extends RecordReader<LongWritable, Text>{

		private LineReader in;
		private BufferedReader buffer, currBuffer;
		private long start, pos, end;
		private LongWritable currentKey = new LongWritable();
		private Text currentValue = new Text();
		
		private String station;
		private String date;
		
		final char endLine = '\n';
		final int MAX_LINE_LENGTH = 150; 
		
		/******debug***********/
		int index = 0;
		/*********************/
		
		@Override
		public void close() throws IOException {
			if (in != null){
				in.close();
			}
			
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO: calculate a value between 0 and 1 that will represent the
	        // fraction of the file that has been processed so far.
			if(start == end){
				return 0.0f;
			}else{
				return Math.min(1.0f, (pos-start)/(float)(end - start));
			}
		}


		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO: read the next key/value, set the key and value variables
	        // to the right values, and return true if there are more key and
	        // to read. Otherwise, return false.
			
			String line = null;
			//Set key 
			currentKey.set(pos);
			String record = "";
			String[] params = null; 
			currentValue.clear();
			
			if(pos >= end){
				System.out.println("End of split...");
				return false;
			}
			currBuffer.mark(MAX_LINE_LENGTH);
			line = currBuffer.readLine();
			
			if(line == null){
				if(getCurrentValue()!= null){
					return true;
				}else{
					return false;
				}
			}
			
			params = line.split(",");
			station = params[0];
			date = params[1];
			
			/***********debug*********/
			if(index < 20){
				System.out.println("Read First line: "+ line);
				index++;
			}
			
			
			//Read the whole block
			while ((station.equals(params[0]))&&(date.equals(params[1]))){
				record += line + endLine;				
				
				currBuffer.mark(MAX_LINE_LENGTH);
				line = currBuffer.readLine();
				if(index < 20){
					System.out.println("Read next line: "+ line);
					index++;
				}

				//Read til the end of file
				if(line != null){
					pos += line.length()+1;				
					params = line.split(",");
				}else{
					//If we reach the end of file, reset params to default
					System.out.println("This is the end of file, don't check anymore");
					pos =end+1;
					params[0]="dummy";
					params[1]="dummy";
				}
			}
			currBuffer.reset();
//			System.out.println(record);
			currentValue.set(new Text(record));
			return true;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Configuration job = context.getConfiguration();
			
			FileSplit fileSplit = (FileSplit)split;
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream is = fs.open(file);
			
			Text line = new Text();
			boolean midLine = false;
			String[] params = null;
			long lineLength = 0;
			String prevStation="dummy";
			String prevDate = "dummy";
			
			
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			is.seek(start);
			in = new LineReader(is, job);
			System.out.println("Initialize a new Split");
			buffer = new BufferedReader(new InputStreamReader(is));
			
			// TODO: write the rest of the function. It will initialize needed
	        // variables, move to the right position in the file, and start
	        // reading if needed.		
			// If Split "S" starts at byte 0, first line will be processed
	        // If Split "S" does not start at byte 0, we need to check if this
			// line has already been processed.
			if(start != 0){
				long prevPos = start;
				char prevChar;
				String prevLine = null;				
				System.out.println("split starts at " + start +", read: "+buffer.readLine());
				
				//Make sure the line did not start at EOL char
				//go back one byte
				prevPos--;
				is.seek(prevPos);
				//read previous byte of the input stream
				buffer = new BufferedReader(new InputStreamReader(is));
				prevChar = (char) buffer.read();
				System.out.println("start reading first backward char: " + prevChar);
				
				//if prev char is EOL, move pointer one byte again
				if(prevChar == endLine){
					System.out.println("This split starts at the beginning of a new line");
					prevPos--;
					is.seek(prevPos);
					buffer = new BufferedReader(new InputStreamReader(is));
					midLine = false;
				}else{
					System.out.println("This split happens middle of sentence");
					midLine = true;
				}
				
				//Read backward until reaches another EOL
				while ((prevChar = (char) buffer.read())!= endLine){
					System.out.print(prevChar);
					--prevPos;					
					is.seek(prevPos);
					buffer = new BufferedReader(new InputStreamReader(is));
				}		
				
				//read the previous line in the previous split, and output key
				prevLine = buffer.readLine();
				System.out.println("The last line of the previous split is: "+prevLine);
				params = prevLine.split(",");
				prevStation = params[0];
				prevDate = params[1];
				System.out.println("prev station: "+prevStation+" prev date: "+prevDate);
			}
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			is.seek(start);
			in = new LineReader(is, job);	
			//if line in the current split is middle of line, read this line and discard it
			if(midLine){
				System.out.println("Starts at the middle of sentence, ignore this line: ");
				start += in.readLine(line);
				System.out.println(line.toString());
			}
			//Read the first complete line
			start += in.readLine(line);
			System.out.println("Read complete line: "+line.toString());
			params = line.toString().split(",");
			station = params[0];
			date = params[1];
			System.out.println("station: "+station+" date: "+date);
			
			//Compare the complete line with previous line
			while (station.equals(prevStation)&&date.equals(prevDate)) {
				System.out.println("line above is ignored");
				lineLength = in.readLine(line);			
				params = line.toString().split(",");
				station = params[0];
				date = params[1];
				System.out.println("Read complete line: "+line.toString());
				System.out.println("station: "+station+" date: "+date);
				start += lineLength;
			}

			//set position to be the beginning of the first distinct line
			this.pos = start-lineLength;
			
			//set start of the buffer, used for next key value pair
			is.seek(pos);
			currBuffer = new BufferedReader(new InputStreamReader(is));
		}
		
	}

}
