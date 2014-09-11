package com.taobao.research.jobs.simrank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.common.IndexValueWritable;

/**
 * 归一化同一Query的所有相似性Queries
 * @author yangxudong.pt
 * @deprecated
 */
public class NormalizeScore
{
	private static class JobPartitioner implements Partitioner<IndexValueWritable, IndexValueWritable>
	{
		public int getPartition(IndexValueWritable key, IndexValueWritable value, int numPartitions)
		{
			int rowIndex = key.index;	//同一行的数据聚集到同一个Reduce
			return (rowIndex & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	//按照组合键的第一个元素（原始键）分组，为了方便对用一个（原始）键对应的值排序
	private static class GroupComparator implements RawComparator<IndexValueWritable>
	{		
		@Override
	    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			int len = Integer.SIZE / 8;
			return WritableComparator.compareBytes(b1, s1, len, b2, s2, len);
	    }		

		@Override
		public int compare(IndexValueWritable rv1, IndexValueWritable rv2)
		{
			if (rv1.index == rv2.index)
				return 0;
			return rv1.index < rv2.index ? -1 : 1;
		}
	}
	
	private static class JobMapper extends MapReduceBase
		implements Mapper<IndexPair, DoubleWritable, IndexValueWritable, IndexValueWritable>
	{
		private IndexValueWritable rowScore = new IndexValueWritable();
		private IndexValueWritable colScore = new IndexValueWritable();
		
		@Override
		public void map(IndexPair indexPair, DoubleWritable score,
				OutputCollector<IndexValueWritable, IndexValueWritable> output,
				Reporter reporter) throws IOException
		{
			rowScore.score = score.get();
			colScore.score = rowScore.score;
			
			rowScore.index = indexPair.index1;	
			colScore.index = indexPair.index2;
			output.collect(rowScore, colScore);
			
			//emit对称矩阵的另一半（上三角矩阵）
			rowScore.index = indexPair.index2;	
			colScore.index = indexPair.index1;
			output.collect(rowScore, colScore);
		}
	}
	
	public static class JobReducer extends MapReduceBase implements
		Reducer<IndexValueWritable, IndexValueWritable, IntWritable, IndexValueWritable>
	{
		private IntWritable key = new IntWritable();	//行号
		private IndexValueWritable value = new IndexValueWritable();
		private double maxScore;
		private IndexValueWritable curValue;
		
		/**
		 * 归一化同一Query的所有相似性Queries
		 * max score --> 1, other score --> other score / max score
		 * @param colValues: 已经倒序排列的相似Queries的scores
		 */
		@Override
		public void reduce(IndexValueWritable rowValue,
				Iterator<IndexValueWritable> colValues,
				OutputCollector<IntWritable, IndexValueWritable> output,
				Reporter arg3) throws IOException
		{
			if (colValues.hasNext())
			{
				key.set(rowValue.index);
				curValue = colValues.next();
				value.index = curValue.index;
				value.score = 1.0d;	//最大的分数排在最前面
				maxScore = curValue.score;
				output.collect(key, value);
			}
			
			while (colValues.hasNext())
			{
				curValue = colValues.next();
				value.index = curValue.index;
				value.score = curValue.score / maxScore;
				output.collect(key, value);
			}
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Normlize Final Score");
		conf.setJarByClass(NormalizeScore.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setPartitionerClass(JobPartitioner.class);
		conf.setMapperClass(JobMapper.class);
		conf.setReducerClass(JobReducer.class);
		conf.setMapOutputKeyClass(IndexValueWritable.class);
		conf.setMapOutputValueClass(IndexValueWritable.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IndexValueWritable.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
	}
	
	public static boolean runJob(String finalScorePath, String outputPath, int R) throws Exception
	{
		System.out.print("************************************************* <normlizing final similarity scores> ");
		System.out.println("*********************************************");

		if (finalScorePath == null || finalScorePath.isEmpty())
			throw new Exception("finalScorePath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		System.out.println("###### inputPath: " + finalScorePath);
		System.out.println("###### outPath: " + outputPath);
		System.out.println("###### reduce num: " + R);
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		finalScorePath = fs.makeQualified(new Path(finalScorePath)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();

		FileInputFormat.addInputPath(job, new Path(finalScorePath));
		FileOutputFormat.setOutputPath(job,	new Path(outputPath));
		
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputPath), true);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
	
	public static void main(String[] args) throws Exception
	{
		String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
		String INPUT_PATH_SCORE = WORK_DIR + "/FinalScore";
		String OUTPUT_PATH_DIR = WORK_DIR + "/NormlizedFinalScore";
		
		runJob(INPUT_PATH_SCORE, OUTPUT_PATH_DIR, 200);
	}
}
