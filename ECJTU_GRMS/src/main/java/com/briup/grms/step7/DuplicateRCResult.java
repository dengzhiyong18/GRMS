package com.briup.grms.step7;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 去重
 * 从用户推荐列表中，剔除用户已购商品
 * 连接
 */
public class DuplicateRCResult extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DuplicateRCResult(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path in1 = new Path(conf.get("in1"));
		Path in2 = new Path(conf.get("in2"));
		Path out = new Path(conf.get("out"));

		Job job = Job.getInstance(conf, "Diyo_DRCResult");
//		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());

		MultipleInputs.addInputPath(job, in1, TextInputFormat.class, DRCRFirstMapper.class);
		MultipleInputs.addInputPath(job, in2, TextInputFormat.class, DRCRSecondMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DRCRReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);

		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/*
	 * 10001 20001 1 10001 20002 1 10001 20005 1 10001 20006 1 10001 20007 1 10002
	 * 20003 1
	 */

	/*
	 * 10001:20001 f10001:20001
	 */
	public static class DRCRFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			context.write(new Text(strs[0] + ":" + strs[1]), new Text());
		}
	}

	/*
	 * 10001:20001 10 10001:20002 11 10001:20003 1 10001:20004 2 10001:20005 9
	 * 10001:20006 10 10001:20007 8 10002:20001 2 10002:20002 2 10002:20003 3
	 */

	/*
	 * 10002:20003 s10002:20003 3
	 */
	public static class DRCRSecondMapper extends Mapper<LongWritable, Text, Text, Text> {
		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			String[] strs = value.toString().split("[\t]");
			context.write(new Text(strs[0]), new Text(strs[1]));
		}
	}

	/*
	 * 10001 20004 2 10001 20003 1 10002 20002 2 10002 20007 2 10002 20001 2 10002
	 * 20005 2 10003 20006 3 10003 20005 3
	 */

	/*
	 * 10001:20001 f10001:20001 10002:20003 s10002:20003 3
	 */
	public static class DRCRReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrayList = new ArrayList<String>();
			for (Text m : values) {
				arrayList.add(m.toString());
			}
			if (arrayList.size() == 1) {
				context.write(key, new Text(arrayList.get(0)));
			}
		}
	}
}
