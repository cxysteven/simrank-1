package com.taobao.research.jobs.common;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * 能够指定最大分片长度的SequenceFileInputFormat 
 * 通过mapred.max.split.size属性指定最大分片大小
 * @author yangxudong.pt
 */
public class SizeCustomizeSequenceFileInputFormat<K, V> extends
		SequenceFileInputFormat<K, V>
{
	// 下面两个变量同FileInputFormat中的定义
	private long minSplitSize = 1;
	private static final double SPLIT_SLOP = 1.1; // 10% slop

	@Override
	public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException
	{

		reporter.setStatus(split.toString());
		FileSplit fs = (FileSplit) split;
		String fileName= fs.getPath().toString();
		job.set("map.input.file", fileName); //使用MultiInputs时，该属性需要自己设置
		return new SequenceFileRecordReader<K, V>(job, (FileSplit) split);
	}
	
	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits)
			throws IOException
	{
		FileStatus[] files = listStatus(job);

		long goalSize = job.getLong("mapred.max.split.size", Long.MAX_VALUE);
		if (goalSize == Long.MAX_VALUE)
		{
			long totalSize = 0; // compute total size
			for (FileStatus file : files)
			{ // check we have valid files
				if (file.isDir())
				{
					throw new IOException("Not a file: " + file.getPath());
				}
				totalSize += file.getLen();
			}
			 goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
		}
		
		long minSize = Math.max(job.getLong("mapred.min.split.size", 1), minSplitSize);

		// generate splits
		ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
		for (FileStatus file : files)
		{
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job);
			long length = file.getLen();
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0,
					length);
			if ((length != 0) && isSplitable(fs, path))
			{
				long blockSize = file.getBlockSize();
				long splitSize = computeSplitSize(goalSize, minSize, blockSize);

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP)
				{
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new FileSplit(path, length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0)
				{
					splits.add(new FileSplit(path, length - bytesRemaining,
							bytesRemaining,	blkLocations[blkLocations.length - 1].getHosts()));
				}
			}
			else if (length != 0)
			{
				splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
			}
			else
			{
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		LOG.debug("Total # of splits: " + splits.size());
		return splits.toArray(new FileSplit[splits.size()]);
	}
}
