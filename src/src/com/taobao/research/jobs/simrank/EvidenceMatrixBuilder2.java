package com.taobao.research.jobs.simrank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.compress.CompressionCodec;
//import org.apache.hadoop.io.compress.GzipCodec;
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

import com.taobao.research.jobs.common.IntArrayWritable;
import com.taobao.research.jobs.common.Join;
import com.taobao.research.jobs.common.LinkWritable;

/**
 * 
 * @author yangxudong.pt
 * @deprecated
 */
public class EvidenceMatrixBuilder2
{	
	public static boolean runJob(String inputPathQas, String inputPathAqs, String outputPath, String workDirPath, int R1, int R2) throws Exception
	{
		System.out.print("------------------------------------------------- <building evidence matrix> ");
		System.out.println("---------------------------------------------");
		if (inputPathQas == null || inputPathQas.isEmpty())
			throw new Exception("inputPathQas is null or empty");
		if (inputPathAqs == null || inputPathAqs.isEmpty())
			throw new Exception("inputPathAqs is null or empty");
		if (outputPath == null || outputPath.isEmpty())
			throw new Exception("outputPath is null or empty");
		
		FileSystem fs = FileSystem.get(new Configuration());
		inputPathQas = fs.makeQualified(new Path(inputPathQas)).toString();
		inputPathAqs = fs.makeQualified(new Path(inputPathAqs)).toString();
		outputPath = fs.makeQualified(new Path(outputPath)).toString();
		
		///1.计算出有共同点击广告的两两Queries
		String queryPairPath = workDirPath + "/QueryPair";
		boolean bRet = GetQueryPair.runJob(inputPathAqs, queryPairPath, R1);
		if (!bRet)
			return false;
		
		///2.转换qas文件的格式，从文本文件转变为Sequence文件，并删除不必要的权值信息
		String qasDataPath = workDirPath + "/qasData";
		bRet = DataFormater.runJob(inputPathQas, qasDataPath, R1);
		if (!bRet)
			return false;
		
		///3.做一次inner join，得到q1-->q2,{a1}格式的文件
		System.out.print("------------------------------------------------- <joining data (1)> ");
		System.out.println("---------------------------------------------");
		String joinTempPath = workDirPath + "/joinTemp";
		fs.delete(new Path(joinTempPath), true);
		String[] joinArgs = {"-r", String.valueOf(R1), "-outKey", IntWritable.class.getName(),
							queryPairPath, qasDataPath, joinTempPath};
		Join.main(joinArgs);

		///4.变换格式，为做第二次inner join做准备，原始格式：q1-->q2,{a1}；目标格式:q2--><q1,{a1}>
		String joinTempPath2 = workDirPath + "/joinTemp2";
		bRet = run(joinTempPath, joinTempPath2, R1);
		if (!bRet)
			return false;
		
		///5.再做一次inner join，得到q2--><q1,{a1}>,{a2}格式的文件
		System.out.print("------------------------------------------------- <joining data (2)> ");
		System.out.println("---------------------------------------------");
		String joinTempPath3 = workDirPath + "/joinTemp3";
		fs.delete(new Path(joinTempPath3), true);
		String[] join2Args = {"-r", String.valueOf(R1), "-outKey", IntWritable.class.getName(),
				joinTempPath2, qasDataPath, joinTempPath3};
		Join.main(join2Args);
		
		///6.读入q2--><q1,{a1}>,{a2}格式的文件，并实际计算出相似性分数
		bRet = EvidenceComputer.runJob(joinTempPath3, outputPath, R2);
		
		///删除中间文件
		fs.delete(new Path(queryPairPath), true);
		fs.delete(new Path(qasDataPath), true);
		fs.delete(new Path(joinTempPath), true);
		fs.delete(new Path(joinTempPath2), true);
		fs.delete(new Path(joinTempPath3), true);
		return bRet;	
	}
	
	/**
	 * 为第二次Join做准备
	 * 输入格式：q1-->q2,{a1}
	 * 输出格式：q2-->q1,{a1}
	 * @author yangxudong.pt
	 *
	 */
	public static class JoinMapper extends MapReduceBase
		implements Mapper<IntWritable, TupleWritable, IntWritable, LinkWritable>
	{
		private LinkWritable output_value = new LinkWritable();
		
		@Override
		public void map(IntWritable qid, TupleWritable value,
				OutputCollector<IntWritable, LinkWritable> output, Reporter arg3)
				throws IOException
		{
			output_value.id = qid.get();
			output_value.array = (IntArrayWritable) value.get(1);
			output.collect((IntWritable) value.get(0), output_value);
		}
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Format justify");
		conf.setJarByClass(EvidenceMatrixBuilder2.class);	
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapperClass(JoinMapper.class);
		conf.setReducerClass(IdentityReducer.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(LinkWritable.class);
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(LinkWritable.class);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 100);
		//对输出进行压缩
//		conf.setBoolean("mapred.output.compress", true);
//		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
//		SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK);
//		SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
	}
	
	public static boolean run(String inputPath, String outputPath, int R) throws Exception
	{
		System.out.print("------------------------------------------------- <preparing for the second join> ");
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
		
		fs.delete(new Path(outputPath), true);
		job.setNumReduceTasks(R);
		configJob(job);
		RunningJob runningJob = JobClient.runJob(job);
		return runningJob.isSuccessful();
	}
	
	public static void main(String[] args) throws Exception
	{
		String INPUT_PATH_QAS = "/group/tbalgo-dev/zhouxj/aqr/20110829/qas/qaspart-00000";
		String INPUT_PATH_AQS = "/group/tbalgo-dev/zhouxj/aqr/20110829/aqs/";
		String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
		String OUTPUT_PATH_DIR = WORK_DIR + "/evidence";
		runJob(INPUT_PATH_QAS, INPUT_PATH_AQS, OUTPUT_PATH_DIR, WORK_DIR, 200, 100);
	}
}
