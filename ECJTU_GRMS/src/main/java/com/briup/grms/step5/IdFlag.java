package com.briup.grms.step5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IdFlag implements WritableComparable<IdFlag>{
	//商品id	连接键
	private	Text gid = new Text();
	//表示来自于某个文件
	private IntWritable flag = new IntWritable();
	
	public IdFlag() {
	}
	public IdFlag(Text gid, IntWritable flag) {
		this.gid = new Text(gid.toString());
		this.flag = new IntWritable(flag.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		gid.readFields(in);
		flag.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		gid.write(out);
		flag.write(out);
	}
	
	//排序
	@Override
	public int compareTo(IdFlag o) {
		return this.gid.compareTo(o.gid)==0 ? this.flag.compareTo(o.flag) : this.gid.compareTo(o.gid);
	}
	
	public Text getGid() {
		return gid;
	}
	/*public void setGid(Text gid) {
		this.gid = new Text(gid);
	}*/

	
}
