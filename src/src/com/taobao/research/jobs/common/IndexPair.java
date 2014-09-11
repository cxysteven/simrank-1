package com.taobao.research.jobs.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** 
 * 矩阵元素的索引，index1和index2分别表示行号和列号
 * 目前数据量的大小还在Integer能表示的范围内，可能以后要改成long型
 */
public class IndexPair implements WritableComparable<IndexPair> 
{
	public int index1;
	public int index2;
	public IndexPair(){}
	public IndexPair(int i1, int i2)
	{
		index1 = i1;
		index2 = i2;
	}
	
	@Override
	public void write (DataOutput out)
		throws IOException
	{
		out.writeInt(index1);
		out.writeInt(index2);
	}
	@Override
	public void readFields (DataInput in)
		throws IOException
	{
		index1 = in.readInt();
		index2 = in.readInt();
	}
	@Override
	public int compareTo (IndexPair o)
	{
		if (this.index1 != o.index1)
			return (this.index1 < o.index1) ? -1 : 1;

		if (this.index2 != o.index2)
			return (this.index2 < o.index2) ? -1 : 1;
		return 0;
	}
	
	/** A Comparator that compares serialized CompositeKey. */
	public static class Comparator extends WritableComparator
	{
		public Comparator()
		{
			super(IndexPair.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
		{
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}

		static
		{ // register this comparator
			WritableComparator.define(IndexPair.class, new Comparator());
		}
	}
	
	@Override
	public IndexPair clone()
	{
		return new IndexPair(index1, index2);
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof IndexPair)
		{
			IndexPair index = (IndexPair)o;
			if (this.index1 == index.index1 && this.index2 == index.index2)
				return true;
		}
		return false;
	}
	@Override
	public int hashCode () {
		return index1 << 16 + index2;
	}
}
