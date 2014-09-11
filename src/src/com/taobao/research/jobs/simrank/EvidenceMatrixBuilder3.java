package com.taobao.research.jobs.simrank;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import com.taobao.research.jobs.common.Common;
import com.taobao.research.jobs.common.IndexPair;

/**
 * 证据矩阵抽取
 * @author yangxudong.pt
 * @create_time 2011-9-15
 */
public class EvidenceMatrixBuilder3
{	
	/**
	 * 求有共同点击广告的两两Query
	 * 实现辅助排序，以便Reduce端的去重
	 * @author yangxudong.pt
	 *
	 */
	private static class QueryPairMapper extends MapReduceBase
 		implements Mapper<LongWritable, Text, IndexPair, IntWritable>
	{
		private Set<Integer> queries = new TreeSet<Integer>();	//存储当前广告对应的所有Queries
		private IndexPair pair = new IndexPair();
		private IntWritable one = new IntWritable(1);
		
		/**
		 * 输入格式：aqs ^A aid ^A {qid ^B click_num}
		 * 输出内容：<qid1,qid2>-->num (二进制）
		 */
		@Override
		public void map(LongWritable lineOffset, Text txtLine,
				OutputCollector<IndexPair, IntWritable> output, Reporter reporter)
				throws IOException
		{
			String strLine = txtLine.toString();
			String[] elements = strLine.split(Common.CONTROL_A);

			if (elements.length <= 2)
				return;
			queries.clear();
			
			String temp;
			for (int i = 2; i < elements.length; i++)
			{
				temp = elements[i].substring(0, elements[i].indexOf(Common.CONTROL_B));
				queries.add(Integer.valueOf(temp));
			}
			
			Integer[] qs = queries.toArray(new Integer[0]);
			int length = queries.size();
			for (int i = 0; i < length - 1; ++i)
			{
				pair.index1 = qs[i];
				for (int j = i + 1; j < length; ++j)
				{
					pair.index2 = qs[j];
					//只输出证据矩阵的下三角矩阵
					output.collect(pair, one); //输出有序的Query-Pair<q1,q2>,其中q1 < q2
				}
			}
		}	
	}

	/**
	 * 执行evidence(a,b)的计算任务
	 * @author yangxudong.pt
	 *
	 */
	private static class QueryPairReducer extends MapReduceBase implements
			Reducer<IndexPair, IntWritable, IndexPair, DoubleWritable>
	{
		private DoubleWritable value = new DoubleWritable();
		// 缓存
		private HashMap<Integer, Double> evidence = new HashMap<Integer, Double>(512);

		/**
		 * evidence(a,b) = ∑1/(2^i), i = 1, 2 ..., size
		 * 
		 * @param size
		 *            : the number of common neighbors between a and b
		 * @return evidence(a,b)
		 */
		private double getEvidence(int size)
		{
			// if (size <= 0)
			// return 0.5d;//size==0时，evidence没有定义，论文中的另一公式计算结果为0，但我觉得不合理

			double e = 0d;
			for (int i = 1; i <= size; ++i)
			{
				e += 1d / Math.pow(2, i);
			}
			return e;
		}

		@Override
		public void reduce(IndexPair pair, Iterator<IntWritable> values,
				OutputCollector<IndexPair, DoubleWritable> output, Reporter arg3)
				throws IOException
		{
			int size = 0; // 两个Query对应的广告集合的交集
			while (values.hasNext())
			{
				size += values.next().get();
			}

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
			output.collect(pair, value);
		}
	}
	
	private static class QueryPairCombiner extends MapReduceBase implements
	Reducer<IndexPair, IntWritable, IndexPair, IntWritable>
	{
		private IntWritable number = new IntWritable();

		@Override
		public void reduce(IndexPair inPair,
				Iterator<IntWritable> values,
				OutputCollector<IndexPair, IntWritable> output, Reporter arg3)
				throws IOException
		{
			int num = 0;
			while (values.hasNext())
			{
				num += values.next().get();
			}
			number.set(num);
			output.collect(inPair, number);
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Evidence Matrix Builder");
		conf.setJarByClass(EvidenceMatrixBuilder3.class);	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapperClass(QueryPairMapper.class);
		conf.setReducerClass(QueryPairReducer.class);
		conf.setCombinerClass(QueryPairCombiner.class);
		conf.setMapOutputKeyClass(IndexPair.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(IndexPair.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		conf.setInt("mapred.reduce.parallel.copies", 8);//Reduce端从Map端并行拷贝数据的并行线程数
		//对map任务输出进行压缩
		conf.setCompressMapOutput(true);
		conf.setMapOutputCompressorClass(GzipCodec.class);
		//对输出进行压缩
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}
	
	public static boolean runJob(String inputPathAqs, String outputDirPath, int R) throws Exception
	{
		System.out.print("************************************************* <building evidence matrix> ");
		System.out.println("*************************************************");

		if (inputPathAqs == null || inputPathAqs.length() == 0)
			throw new Exception("inputPathAqs is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");		
		
		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		inputPathAqs = fs.makeQualified(new Path(inputPathAqs)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		System.out.println("###### inputPathAqs: " + inputPathAqs);
		System.out.println("###### outputPath: " + outputDirPath);
		System.out.println("###### reduce num: " + R);
		
		FileInputFormat.addInputPath(job, new Path(inputPathAqs));
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
		String OUTPUT_PATH_EVIDENCE = WORK_DIR + "/evidence";
		String INPUT_PATH_AQS = "/group/tbalgo-dev/zhouxj/aqr/20110829/aqs/";

		
		runJob(INPUT_PATH_AQS, OUTPUT_PATH_EVIDENCE, 400);
	}
}
