package com.taobao.research.jobs.common;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;

public class GenericObject extends GenericWritable
{
	private static Class[] CLASSES = { IndexValueWritable.class, Text.class };

	protected Class[] getTypes()
	{
		return CLASSES;
	}
}
