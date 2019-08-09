package com.briup.grms.step5;

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
 * 计算对于某个用户推荐某个商品的推荐值
 * 输入	step3结果	step4结果
 * 进行连接操作 reduce端连接
 * 1.借助 MultipleInputs 类把两个Mapper输出的结果汇聚到同一个reduce中
 * 2. 二次排序
 * 		i 构建复合键		Comparable即比较复合键也比较复合值
 * 		ii 分区比较器  只比较复合键中的自然键
 * 		iii 分组比较器	只比较复合键中的自然键
 * 结果		g1,u1	1
 * 			g1,u2	2
 * 			g1,u1	3
 */
public class GetRecmandResult1 extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GetRecmandResult1(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance
				(conf,"DZY_GetRecmandResult");
			job.setJarByClass(this.getClass());

			job.setMapperClass(GRRFirstMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setMapperClass(GRRSecondMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(GRRVectorReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass
				(TextInputFormat.class);
			TextInputFormat.addInputPath
				(job,new Path(conf.get("inpath")));
			
			job.setInputFormatClass
			(TextInputFormat.class);
		TextInputFormat.addInputPath
			(job,new Path(conf.get("inpath2")));


			job.setOutputFormatClass
				(TextOutputFormat.class);
			TextOutputFormat.setOutputPath
				(job,new Path(conf.get("outpath")));

//			job.waitForCompletion(true);
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class GRRFirstMapper extends Mapper<LongWritable,Text,Text,Text> {
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] strs = value.toString().split("\t");
	        String s =   "f"+strs[1];
	        context.write(new Text(strs[0]),new Text(s));
	    }
	}
	
	
	
	public static class GRRSecondMapper extends Mapper<LongWritable,Text,Text,Text> {
	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] strs = value.toString().split("\t");
	        context.write(new Text(strs[0]),new Text(strs[1]));
	    }
	}
	
	public static class GRRVectorReducer extends Reducer<Text,Text,Text,Text> {
	    @Override
	    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        String f = "";
	        String v = "";
	        String s = "" ;
	        String mname,nnmae;
	        int mnumber,nnumber;
	        for (Text value : values) {
	            v = value.toString();
	            if (v.substring(0,1).equals("f")){
	                f = v.substring(1);
	            }
	            else {
	                s = v;
	            }
	        }
	        String[] f1 = f.split(",");
	        String[] v1 = s.split(",");
	        for (String m:f1){
	            mname=m.split(":")[0];
	            mnumber=Integer.parseInt(m.split(":")[1]);
	            for (String n:v1){
	                nnmae = n.split(":")[0];
	                nnumber = Integer.parseInt(n.split(":")[1]);
	                context.write(new Text(nnmae+":"+mname), new Text(String.valueOf(mnumber*nnumber)));
	            }
	        }
	    }
	}

}
