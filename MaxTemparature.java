package com.mapr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class MaxTemperatureMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public MaxTemperatureMapper() {
		System.out.println("MaxTemperatureMapper.MaxTemperatureMapper()");
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureMapper.setup()");
		super.setup(context);
	}

	@Override
	public void run(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureMapper.run()");
		super.run(context);
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureMapper.cleanup()");
		super.cleanup(context);
	}
/*
 * 
0067011990999991950051507004...9999999N9+00001+99999999999...
0043011990999991950051512004...9999999N9+00221+99999999999...
0043011990999991950051518004...9999999N9-00111+99999999999...
0043012650999991949032412004...0500001N9+01111+99999999999...
0043012650999991949032418004...0500001N9+00781+99999999999...
 * 
 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		System.out.println("MaxTemperatureMapper.map() line :: "+ line);
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(40) == '+') { // parseInt doesn't like leading plus signs
			airTemperature = Integer.parseInt(line.substring(41, 46));
		} else {
			airTemperature = Integer.parseInt(line.substring(40, 46));
		}
		System.out.println("MaxTemperatureMapper.map() year | airTemperature :: "+year +" | "+ airTemperature );
		context.write(new Text(year), new IntWritable(airTemperature));
	}

}

class MaxTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void cleanup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureReducer.cleanup()");
		super.cleanup(context);
	}

	@Override
	protected void setup(
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureReducer.setup()");
		super.setup(context);
	}

	@Override
	public void run(Reducer<Text, IntWritable, Text, IntWritable>.Context arg0)
			throws IOException, InterruptedException {
		System.out.println("MaxTemperatureReducer.run()");
		super.run(arg0);
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		System.out.println("MaxTemperatureReducer.reduce() key | maxValue ::  "+key +" | "+maxValue);
		context.write(key, new IntWritable(maxValue));
	}
}

public class MaxTemparature {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		try {
			Job job = Job.getInstance(new Configuration(), "Max temperature");
			job.setJarByClass(MaxTemparature.class);
			job.setJobID(new JobID("Max temperature", 1));
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(MaxTemperatureMapper.class);
			job.setCombinerClass(MaxTemperatureReducer.class);
			job.setNumReduceTasks(3);
			job.setReducerClass(MaxTemperatureReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IllegalArgumentException | IOException
				| ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}

	}

}
