package com.taobao.research.jobs.simrank;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import com.taobao.research.jobs.common.Common;
import com.taobao.research.jobs.common.IntArrayWritable;

/**
 * 把qas或aqs数据转换为二进制格式，方便压缩
 * 在计算证据矩阵时，需要用到
 * @author yangxudong.pt
 * @deprecated
 */
public class DataFormater
{
	private static class JobMapper extends MapReduceBase
 		implements Mapper<LongWritable, Text, IntWritable, IntArrayWritable>
	{
		private IntWritable key = new IntWritable();
		private IntArrayWritable value = new IntArrayWritable();
		
		/**
		 * 输入格式1：aqs ^A aid ^A {qid ^B click_num}
		 * 输出内容1：aid,{qid} (二进制）
		 * 输入格式2：qas ^A qid ^A {aid ^B click_num}
		 * 输出内容2：qid,{aid} (二进制）
		 */
		@Override
		public void map(LongWritable lineOffset, Text line,
				OutputCollector<IntWritable, IntArrayWritable> output, Reporter reporter)
				throws IOException
		{
			String strLine = line.toString();
			String[] elements = strLine.split(Common.CONTROL_A);

			if (elements.length <= 2)
				return;
			
			int id = Integer.parseInt(elements[1]);
			key.set(id);
			value.initialize(elements.length - 2);
			String temp;
			for (int i = 2; i < elements.length; i++)
			{
				temp = elements[i].substring(0, elements[i].indexOf(Common.CONTROL_B));
				value.content[i - 2] = Integer.parseInt(temp);
			}
			output.collect(key, value);
		}
			
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Format Transformer");
		conf.setJarByClass(DataFormater.class);	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapperClass(JobMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(IntArrayWritable.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntArrayWritable.class);
		conf.set("mapred.child.java.opts", "-Xmx3000m");
		conf.setInt("io.sort.mb", 512);
		conf.setInt("io.sort.factor", 100);
		//对输出进行压缩
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}
	
	public static boolean runJob(String inputPath, String outputPath, int R) throws Exception
	{
		System.out.print("------------------------------------------------- <transforming Format between Queries and Ads> ");
		System.out.println("---------------------------------------------");
		if (inputPath == null || inputPath.isEmpty())
			throw new Exception("inputPath is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPath = fs.makeQualified(new Path(inputPath)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job,	new Path(outputPath));
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputPath), true);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();	
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception
	{
		String inputPath = args[0];
		String outputPath = args[1];
		runJob(inputPath, outputPath, 10);
	}
}
