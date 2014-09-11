package com.taobao.research.jobs.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class IndexValueWritable implements WritableComparable<IndexValueWritable>
{
	public int index;	//表示矩阵的行号或列号
	public double score;
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		index = in.readInt();
		score = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(index);
		out.writeDouble(score);
	}

	@Override
	public int compareTo(IndexValueWritable o)
	{
		if (index != o.index)
			return index < o.index ? -1 : 1;
		if (score != o.score)
			return score < o.score ? 1 : -1;	//按分数降序排列
		return 0;
	}
	
	/** A Comparator that compares serialized CompositeKey. */
	public static class Comparator extends WritableComparator
	{
		public Comparator()
		{
			super(IndexValueWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
		{
			int r1 = readInt(b1, s1);
			int r2 = readInt(b2, s2);
			if (r1 != r2)
				return r1 < r2 ? -1 : 1;
			
			int len = Integer.SIZE / 8;
			double sc1 = readDouble(b1, s1 + len);
			double sc2 = readDouble(b2, s2 + len);
			if (sc1 == sc2)
				return 0;
			
			return sc1 < sc2 ? 1 : -1;	//按分数降序排列
		}

		static
		{ // register this comparator
			WritableComparator.define(IndexValueWritable.class, new Comparator());
		}
	}
}