package com.briup.grms.step7;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 去重
 * 从用户推荐列表中，剔除用户已购商品
 *   step6		/data/rmc/process/matrix_data.txt
 * 
 * 
 * 连接	MultipleInputs
 * 
 * TextInputFormat.addInputPaths()
 */
public class DuplicateRCResultT extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DuplicateRCResultT(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Diyo_DRCResultT_NEW");
		job.setJarByClass(this.getClass());
		
		Path in1 = new Path(conf.get("inpath1"));
		Path in2 = new Path(conf.get("inpath2"));
		Path out = new Path(conf.get("outpath"));

		MultipleInputs.addInputPath(job, in1, TextInputFormat.class, DRCRTFirstMapper.class);
		MultipleInputs.addInputPath(job, in2, KeyValueTextInputFormat.class, DRCRTSecondMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DRCRTReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	//uid	gid	->	v
	public static class DRCRTFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String infos[] = value.toString().split(" ");
			String pair = infos[1]+","+infos[0];
			context.write(new Text(pair), new Text(infos[2]));
		}
	}

	//gid	uid		
	//次Mapper不用装配 使用原始Mapper
	public static class DRCRTSecondMapper extends Mapper<Text, Text, Text, Text> {
		
	}

	
	public static class DRCRTReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> ite = values.iterator();
			String v1 = ite.next().toString();
			if (ite.hasNext()) {
				return;
			}
			//方便入库
			String[] gus = key.toString().split(",");
			context.write(new Text(gus[0]+"\t"+gus[1]), new Text(v1));
		}
	}
}
