package com.mapr.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

class GroupByColorMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		context.write(new Text(values[3]), new Text(values[1] + "," + values[2]));
	}
}

class GroupByColorReducer extends Reducer<Text, Text, Text, NullWritable> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(
					new Text(value.toString().substring(0,
							value.toString().indexOf(','))
							+ key
							+ value.toString().substring(
									value.toString().indexOf(',') + 1)),
					NullWritable.get());
		}
	}
}

public class GroupByColorDriver extends Configured implements Tool {

	public static void main(String[] args) {
		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(new Configuration(),
					new GroupByColorDriver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("GroupByColorDriver\n");
			return -1;
		}

		Job job = Job.getInstance(getConf(), "GroupByColorDriver");

		job.setJarByClass(GroupByColorDriver.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(GroupByColorMapper.class);
		job.setReducerClass(GroupByColorReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
