package com.briup.grms.step4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 获得用户购买向量的 顺时针旋转90°结果
 * 输入
 * 输出 grms/step4
 * 
 * 结果
 * 20001	10001:1,10002:2....
 */
public class PurchasedGoodsVector extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PurchasedGoodsVector(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance
				(conf,"DZY_PurchasedGoodsVector");
			job.setJarByClass(this.getClass());

			job.setMapperClass(PGVMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(PGVReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass
				(TextInputFormat.class);
			TextInputFormat.addInputPath
				(job,new Path(conf.get("inpath")));

			job.setOutputFormatClass
				(TextOutputFormat.class);
			TextOutputFormat.setOutputPath
				(job,new Path(conf.get("outpath")));

//			job.waitForCompletion(true);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class PGVMapper extends Mapper<LongWritable,Text,Text,Text> {
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String[] strs = value.toString().split(" ");
		        context.write(new Text(strs[1]),new Text(strs[0]));
		    }
	}
	
	public static class PGVReducer extends Reducer<Text,Text,Text,Text> {
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	       StringBuffer vector = new StringBuffer();
	        values.forEach(s -> vector.append(s.toString()).append(":").append(1).append(","));
	        context.write(key,new Text(vector.substring(0,vector.length() -1)));
/*	        StringBuilder sb = new StringBuilder();
	        for (Text value : values) {
	        	sb.append(value).append(":").append(1).append(",");
	        }
	        context.write(key,new Text(sb.toString().substring(0,sb.length()-1)));
*/	    }
	}
	
	
}
