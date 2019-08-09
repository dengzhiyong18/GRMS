package com.briup.grms.step6;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 把step5计算结果求和
 * gid,uid	v
 */
public class RecmandResultSumT extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RecmandResultSumT(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Diyo_RecmandResultSumT");
		job.setJarByClass(this.getClass());

//		job.setMapperClass(RRSMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(RRSTReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/*public static class RRSMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splits = line.split("\t");
			context.write(new Text(splits[0]), new IntWritable(Integer.parseInt(splits[1])));
		}
	}*/

	public static class RRSTReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Integer sum = StreamSupport
					.stream(values.spliterator(), false)
					.map(s -> Integer.parseInt(s.toString()))
					.reduce((x,y) -> x+y).get();
			context.write(key, new Text(sum+""));
		}
	}

}
