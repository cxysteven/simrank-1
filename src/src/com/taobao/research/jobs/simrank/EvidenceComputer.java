package com.taobao.research.jobs.simrank;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.common.IntArrayWritable;
import com.taobao.research.jobs.common.LinkWritable;


/**
 * 执行evidence(a,b)的计算任务
 * @author yangxudong.pt
 * @deprecated
 */
public class EvidenceComputer
{
	private static class JobMapper extends MapReduceBase
		implements Mapper<IntWritable, TupleWritable, IndexPair, DoubleWritable>
	{
		//缓存
		private HashMap<Integer, Double> evidence = new HashMap<Integer, Double>(512);
		private IndexPair index = new IndexPair();
		private DoubleWritable value = new DoubleWritable();
		/**
		 * evidence(a,b) = ∑1/(2^i), i = 1, 2 ..., size
		 * @param size: the number of common neighbors between a and b
		 * @return evidence(a,b)
		 */
		private double getEvidence(int size)
		{
//			if (size <= 0)
//				return 0.5d;//size==0时，evidence没有定义，论文中的另一公式计算结果为0，但我觉得不合理
			
			double e = 0d;
			for (int i = 1; i <= size; ++i)
			{
				e += 1d / Math.pow(2, i);
			}
			return e;
		}
		
		//求交集的大小
		private int getIntersectionSize(IntArrayWritable lhs, IntArrayWritable rhs)
		{
			Arrays.sort(lhs.content);
			Arrays.sort(rhs.content);
			int count = 0;
			int l = 0, r = 0;
			while (l < lhs.length && r < rhs.length)
			{
				if (lhs.content[l] == rhs.content[r])
				{
					++count;
					++l;
					++r;
				}
				else if (lhs.content[l] < rhs.content[r])
					++l;
				else
					++r;
			}
			
			return count;
		}

		@Override
		public void map(IntWritable q2, TupleWritable tuple,
				OutputCollector<IndexPair, DoubleWritable> output, Reporter reporter)
				throws IOException
		{
			index.index2 = q2.get();
			LinkWritable q1as = (LinkWritable) tuple.get(0);
			IntArrayWritable a2s = (IntArrayWritable) tuple.get(1);
			index.index1 = q1as.id;
			
			int size = getIntersectionSize(q1as.array, a2s);
			if (size <= 0)
				return;
			Integer intSize = Integer.valueOf(size);
			
			if (evidence.containsKey(intSize))
			{
				value.set(evidence.get(intSize));
			}
			else
			{
				double e = getEvidence(size);
				evidence.put(intSize, Double.valueOf(e));
				value.set(e);
			}
			output.collect(index, value);
			//emit对称的值，方便以后和相似性分数做Join
			index.index1 = index.index2;
			index.index2 = q1as.id;
			output.collect(index, value);
			//if (q2.get() < 500)
			//{
				//System.out.println(index.index1 + "," + index.index2 + "->" + value.get());
			//}		
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Comput Evidence");
		conf.setJarByClass(EvidenceComputer.class);	
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapperClass(JobMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		conf.setMapOutputKeyClass(IndexPair.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputKeyClass(IndexPair.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		//对输出进行压缩
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}
	
	public static boolean runJob(String inputPath, String outputDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <computing evidence pairs> ");
		System.out.println("*************************************************");

		if (inputPath == null || inputPath.length() == 0)
			throw new Exception("inputPath is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");		
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPath = fs.makeQualified(new Path(inputPath)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job,	new Path(outputDirPath));
		
		job.setNumReduceTasks(R);
		fs.delete(new Path(outputDirPath), true);	
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
	
	public static void main(String[] args) throws Exception
	{
		String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
		String INPUT_PATH = WORK_DIR + "/joinTemp3";
		String OUTPUT_PATH_DIR = WORK_DIR + "/evidence";
		runJob(INPUT_PATH, OUTPUT_PATH_DIR, 600);
	}
}
