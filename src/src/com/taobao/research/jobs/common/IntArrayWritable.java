package com.taobao.research.jobs.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable implements Writable
{
	public int length;		//正数表示数组长度，负数和零仅表示一个值，没有数组内容
	public int[] content;	//数组内容
	
	public void initialize(int len)
	{
		length = len;
		if (len > 0)
			content = new int[length];
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		length = in.readInt();
		if (length <= 0)
			return;
		content = new int[length];
		for (int i = 0; i < length; i++)
		{
			content[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		out.writeInt(length);
		for (int i = 0; i < length; i++)
		{
			out.writeInt(content[i]);
		}
	}
	
	@Override
	public IntArrayWritable clone()
	{
		IntArrayWritable array = new IntArrayWritable();
		array.length = length;
		if (length > 0)
		{
			array.content = new int[length];
			for (int i = 0; i < length; i++)
			{
				array.content[i] = content[i];
			}
		}
		return array;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof IntArrayWritable)
		{
			IntArrayWritable array = (IntArrayWritable)o;
			if (length != array.length)
				return false;

			for (int i = 0; i < length; i++)
			{
				if (content[i] != array.content[i])
					return false;
			}
			return true;
		}
		return false;	
	}
}