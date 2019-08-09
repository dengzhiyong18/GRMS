package com.briup.grms.step2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 计算两两商品共现次数
 * 输入数据		./grms/step1/
 * 输出数据		./grms/step2/
 *10001		20001,20002,20003.。。。。。
 *
 *20001		20002	3
 *20002		20004	6
 *.......
 *
 *思考？
 *下一步我们需要把共现列表整理成矩阵形式
 *考虑这一步，怎么做能更方便下一步操作
 *
 */
public class GoodsCooccurrenceList1 extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new GoodsCooccurrenceList1(), args);
	}
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf,"DZY_GoodsCooccurrenceList");
		
		job.setJarByClass(this.getClass());

		job.setMapperClass(GCLMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(GCLReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass
			(TextInputFormat.class);
		TextInputFormat.addInputPath
			(job,new Path(conf.get("inpath")));

		job.setOutputFormatClass
			(TextOutputFormat.class);
		TextOutputFormat.setOutputPath
			(job,new Path(conf.get("outpath")));

		job.waitForCompletion(true);
		return 0;
	}
	
	public static class GCLMapper
	extends Mapper<LongWritable,Text,
		Text,IntWritable>{
	@Override
	protected void map(LongWritable key,
	    Text value, Context context) throws IOException, InterruptedException {
		/*String[] strs = value.toString().split("\t");
        String[] s = strs[1].split(",");
        for (String s1 : s) {
            for (String s2 : s) {
                context.write(new Text(s1),new IntWritable(s2));
            }
        }*/
		String line = value.toString();
		String[] split = line.split(",");
		for (int i = 1; i < split.length; i++) {
			for (int j = 1; j < split.length; j++) {
				String pair = null;
				pair = split[i]+"\t"+split[j];
				context.write(new Text(pair), new IntWritable(1));
			}
		}
	}
}

public static class GCLReducer extends
		Reducer<Text,IntWritable,Text,IntWritable>{
	@Override
	protected void reduce
		(Text key, Iterable<IntWritable> values,
		 Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			 sum += value.get();
            context.write(key,new IntWritable(sum));
        }
	}
}

}
