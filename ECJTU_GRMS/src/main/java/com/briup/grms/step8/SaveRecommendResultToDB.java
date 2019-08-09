package com.briup.grms.step8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SaveRecommendResultToDB extends Configured implements Tool{
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf=getConf();
        Path in=new Path(conf.get("inpath"));

//        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        Job job=Job.getInstance(conf,"Diyo_SRRToDB");
        job.setJarByClass(this.getClass());

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job,in);

        job.setReducerClass(SRRToDBReducer.class);
        job.setOutputKeyClass(Results.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setOutputFormatClass(DBOutputFormat.class);
        DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver","jdbc:mysql://172.16.0.5:3306/grms?characterEncoding=UTF-8","briup","briup");
        DBOutputFormat.setOutput(job,"hj_Diyo_grms","uid","gid","exp");
        
//        DBConfiguration.configureDB(job.getConfiguration(),"oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@127.0.0.1:1521:XE","briup","briup");
//        DBOutputFormat.setOutput(job,"results","user_id","gid","exp");
        
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SaveRecommendResultToDB(),args));
    }
    
    

    public static class SRRToDBMapper extends Mapper<Text, Text, Text, Text>{
    	
    }
    
    public static class SRRToDBReducer extends Reducer<Text, Text, Results, NullWritable>{
    	@Override
    	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Results, NullWritable>.Context context)
    			throws IOException, InterruptedException {
    		String uid = "" ;
            String gid = "" ;
            int exp = 0 ;
            for (Text value : values) {
				String[] s = value.toString().split("\t");
				uid = s[1];
				gid = s[0];
				exp = Integer.parseInt(s[2]);
				context.write(new Results(uid, gid, exp), NullWritable.get());
			}
    		
    	}
    }
    
    /*public static class SRRToDBMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(), value);
        }
    }*/
    
    /*public static class SRRToDBReducer extends Reducer<Text,Text,Results,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String uid = "" ;
            String gid = "" ;
            int exp = 0 ;
            for (Text value : values) {
                String[] s = value.toString().split("\t");
                String[] v = s[0].split("\t");
                uid = v[0];
                gid = v[1];
                exp = Integer.parseInt(s[1]);
                context.write(new Results(uid,gid,exp),NullWritable.get());
                uid = s[0];
                gid = s[1];
                exp = Integer.parseInt(s[2]);
                context.write(new Results(uid,gid,exp),NullWritable.get());
            }
        }
    }*/
    
}
