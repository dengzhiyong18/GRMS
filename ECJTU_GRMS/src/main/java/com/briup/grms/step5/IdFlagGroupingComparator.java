package com.briup.grms.step5;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//分组比较器
public class IdFlagGroupingComparator extends WritableComparator{

	public IdFlagGroupingComparator() {
		super(IdFlag.class,true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IdFlag i1 = (IdFlag) a;
		IdFlag i2 = (IdFlag) b;
		return i1.getGid().compareTo(i2.getGid());
	}

	
}
