package com.mapr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MRJoin {
	
	
	public static class CustMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] custArray = value.toString().split(",");
	        int custIdInt = Integer.parseInt(custArray[0]);
	        String custNameStr = custArray[1];
	        IntWritable custId = new IntWritable(custIdInt);
	        Text custName = new Text("c "+custNameStr);
	        context.write(custId, custName);
	    }
	}
	               
	public static class TxMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	 	String[] txArray = value.toString().split(",");
		        int custIdInt = Integer.parseInt(txArray[1]);
		        IntWritable custId = new IntWritable(custIdInt);
		        Text txAmt = new Text("t "+txArray[1]);
		        context.write(custId, txAmt);
	    }
	}
	
	public static class CustTxJoinRecuder extends Reducer<IntWritable, Text, Text,NullWritable>{
	    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	        String joinStr = "";
	        Float amtTemp = new Float(0.0);
	    	for(Text valCT : values){
	    		if(valCT.toString().startsWith("c ")){
	    			joinStr = joinStr + key + " " + valCT;
	    		}else if(valCT.toString().startsWith("t ")){
	    			amtTemp = amtTemp + Float.valueOf(valCT.toString());
	    		}else{
	    			// do nothing
	    		}
	    	}
	    	joinStr = joinStr + " "+ amtTemp;
	    }
	}

	public static void main(String[] args) {

		
	}

}
