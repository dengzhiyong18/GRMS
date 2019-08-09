package com.briup.grms.step1;

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

import java.io.IOException;

/**
 * 计算用户已购商品列表 输入数据 公司hdfs /data/rmc/process/matrix_data.txt 输出数据 ./grms/step1
 * 10001 20001 1 10001 20002 1 10001 20003 1
 *
 * 10001 20001,20002,20003...... extends Configured 获取配置对象 implements Tool
 * 命令行使用-D形式传参给代码
 */
public class PurchasedGoodsList extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PurchasedGoodsList(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "DZY_PurchasedGoodsList");
		job.setJarByClass(this.getClass());

		job.setMapperClass(PGLMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(PGLReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

		job.waitForCompletion(true);
		return 0;
	}

	public static class PGLMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] infos = value.toString().split(" ");
			context.write(new Text(infos[0]), new Text(infos[1]));
		}
	}

	public static class PGLReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			values.forEach(i -> sb.append(i).append(","));
			String str = sb.substring(0, sb.length() - 1);
			context.write(key, new Text(str));
		}
	}
}
