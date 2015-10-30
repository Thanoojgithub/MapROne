package com.mapr.secondarysort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/*
 * http://hadooped.blogspot.in/2013/09/sample-code-for-secondary-sort.html
 * https://gist.github.com/airawat
 */

class CompositeKeyWritable implements Writable,
		WritableComparable<CompositeKeyWritable> {

	private String deptNo;
	private String fNamelNameEmpIDPair;

	public CompositeKeyWritable() {
	}

	public CompositeKeyWritable(String deptNo, String fNamelNameEmpIDPair) {
		this.deptNo = deptNo;
		this.fNamelNameEmpIDPair = fNamelNameEmpIDPair;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(deptNo).append("\t")
				.append(fNamelNameEmpIDPair)).toString();
	}

	public void readFields(DataInput dataInput) throws IOException {
		deptNo = WritableUtils.readString(dataInput);
		fNamelNameEmpIDPair = WritableUtils.readString(dataInput);
	}

	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, deptNo);
		WritableUtils.writeString(dataOutput, fNamelNameEmpIDPair);
	}

	public int compareTo(CompositeKeyWritable objKeyPair) {
		String objKeyPairAttributes[] = objKeyPair.getFNamelNameEmpIDPair()
				.toString().split("\t");
		String fNamelNameEmpIDPairAttributes[] = fNamelNameEmpIDPair.toString()
				.split("\t");
		
		int result = fNamelNameEmpIDPairAttributes[0].compareTo(objKeyPairAttributes[0]);
			if(0 ==result){
				result = fNamelNameEmpIDPairAttributes[1].compareTo(objKeyPairAttributes[1]);
				if(0 ==result){
					result = fNamelNameEmpIDPairAttributes[2].compareTo(objKeyPairAttributes[2]);
				}
			}
		
		 /*
		  * 	String objKeyPairAttributes[] = objKeyPair.getFNamelNameEmpIDPair()
				.toString().split("\t");
		String fNamelNameEmpIDPairAttributes[] = fNamelNameEmpIDPair.toString()
				.split("\t");
		
		  * return ComparisonChain
				.start()
				.compare(objKeyPair.deptNo, deptNo)
				.compare(objKeyPairAttributes[0],
						fNamelNameEmpIDPairAttributes[0])
				.compare(objKeyPairAttributes[1],
						fNamelNameEmpIDPairAttributes[1])
				.compare(objKeyPairAttributes[2],
						fNamelNameEmpIDPairAttributes[2]).result();*/
		
		return result;
	}

	public String getDeptNo() {
		return deptNo;
	}

	public void setDeptNo(String deptNo) {
		this.deptNo = deptNo;
	}

	public String getFNamelNameEmpIDPair() {
		return fNamelNameEmpIDPair;
	}

	public void setFNamelNameEmpIDPair(String fNamelNameEmpIDPair) {
		this.fNamelNameEmpIDPair = fNamelNameEmpIDPair;
	}
}

class SecondarySortBasicPartitioner extends
		Partitioner<CompositeKeyWritable, NullWritable> {

	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value,
			int numReduceTasks) {

		return (key.getDeptNo().hashCode() % numReduceTasks);
	}
}

class SecondarySortBasicGroupingComparator extends WritableComparator {
	protected SecondarySortBasicGroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key2.getDeptNo().compareTo(key1.getDeptNo());
	}
}

class SecondarySortBasicMapper extends
		Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString().length() > 0) {
			String arrEmpAttributes[] = value.toString().split(",");

			context.write(
					new CompositeKeyWritable(
							arrEmpAttributes[6].toString(),
							(arrEmpAttributes[2].toString() + "\t"
									+ arrEmpAttributes[3].toString() + "\t" + arrEmpAttributes[0]
									.toString())), NullWritable.get());
		}

	}
}



class SecondarySortBasicReducer
		extends
		Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {

	@SuppressWarnings("static-access")
	@Override
	public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		for (NullWritable value : values) {
			context.write(key, value.get());
		}

	}
}

public class SecondarySortBasicDriver extends Configured implements Tool {

	public static void main(String[] args) {
		int exitCode = 0;
		try {
			exitCode = ToolRunner.run(new Configuration(),
					new SecondarySortBasicDriver(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(exitCode);

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for SecondarySortBasicDriver- <input dir> <output dir>\n");
			return -1;
		}

		Job job = Job.getInstance(getConf(), "Secondary sort example");

		job.setJarByClass(SecondarySortBasicDriver.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SecondarySortBasicMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(SecondarySortBasicPartitioner.class);
		job.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
		job.setReducerClass(SecondarySortBasicReducer.class);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(8);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
