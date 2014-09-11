package com.taobao.research.jobs.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LinkWritable implements Writable
{
	public int id;
	public IntArrayWritable array = new IntArrayWritable();
	
	@Override
	public LinkWritable clone()
	{
		LinkWritable v = new LinkWritable();
		v.id = id;
		v.array = (IntArrayWritable) array.clone();
		return v;	
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof LinkWritable)
		{
			LinkWritable v = (LinkWritable)o;
			if (id == v.id)
				return true;	//认为数据是正确的，只要id相同，则对应的值也相同
//			if (array.equals(v.array))
//				return true;
		}
		return false;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		id = in.readInt();
		array.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(id);
		array.write(out);
	}	
}
