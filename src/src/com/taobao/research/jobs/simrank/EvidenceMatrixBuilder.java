package com.taobao.research.jobs.simrank;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import com.sun.corba.se.impl.ior.ByteBuffer;
import com.taobao.research.jobs.common.Common;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.common.IntArrayWritable;

/**
 * 构建证据矩阵
 * @author yangxudong.pt
 * @deprecated
 */
public class EvidenceMatrixBuilder
{
	private static final String algo = "MD5";	//修改此值的同时需要修改digest_length的值
	private static int digest_length = 16; //摘要长度，和算法相关
	
	public static class QuerySetDigest implements WritableComparable<QuerySetDigest>
	{
		private byte[] digest;	//存储Query Set的信息摘要
		private Set<Integer> querySet; //存储Query Set
		private MessageDigest digestGenerater;
		
		public QuerySetDigest()
		{
			try
			{
				digestGenerater = MessageDigest.getInstance(algo);
				digest_length = digestGenerater.getDigestLength();
				querySet = new TreeSet<Integer>(); //TreeSet保证加进去的集合是有序的
			}
			catch (NoSuchAlgorithmException e)
			{
				e.printStackTrace();
			}
		}
		
		public byte[] getDigest()
		{
			return digest;
		}
		
		public boolean addQueryID(int id)
		{
//			if (querySet.contains(id))
//				return true;
			return querySet.add(id);	//Set数据结构保证不会加进去相同的元素
		}
		
		public void generateDigest()
		{
			ByteBuffer buffer = new ByteBuffer();
			Iterator<Integer> iter = querySet.iterator();
			while (iter.hasNext())
			{
				buffer.append(iter.next());
			}
			
			digestGenerater.update(buffer.toArray());
			digest = digestGenerater.digest();
		}
		
		public void reset()
		{
			querySet.clear();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			digest = new byte[digest_length];
			in.readFully(digest);
		}

		@Override
		public void write(DataOutput out) throws IOException
		{
			out.write(digest);	
		}

		@Override
		public int compareTo(QuerySetDigest o)
		{
			byte[] d2 = o.getDigest();	
			return WritableComparator.compareBytes(digest, 0, digest.length, d2, 0, d2.length);
		}
		
		/** A Comparator that compares serialized CompositeKey. */
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(QuerySetDigest.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)					
			{
				return compareBytes(b1, s1, l1, b2, s2, l2);
			}

			static
			{ // register this comparator
				WritableComparator.define(QuerySetDigest.class, new Comparator());
			}
		}	
	}
	
	
	
	public static class Value implements Writable
	{
		public int id;
		public IntArrayWritable array = new IntArrayWritable();
		
		@Override
		public Value clone()
		{
			Value v = new Value();
			v.id = id;
			v.array = (IntArrayWritable) array.clone();
			return v;	
		}
		
		@Override
		public boolean equals(Object o)
		{
			if (o instanceof Value)
			{
				Value v = (Value)o;
				if (id == v.id)
					return true;	//认为数据是正确的，只要id相同，则对应的值也相同
//				if (array.equals(v.array))
//					return true;
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
	
	private static class JobMapper extends MapReduceBase
 	implements Mapper<LongWritable, Text, QuerySetDigest, Value>
	{
		//改为数组可能更好
		//private Map<Integer, IntArrayWritable> aqsData = new HashMap<Integer, IntArrayWritable>();
		private IntArrayWritable[] aqsData;
		private ArrayList<Value> values;
		private QuerySetDigest querySet;
		private int ad_num;
		private String inputAqsData;
		
		/**
		 * 从DistributedCache里解压缩文件并构建Map
		 * @param file
		 * @throws IOException
		 */
		/*		private void buildData(Path file, JobConf job)
		{
//			Configuration conf = new Configuration();
			CompressionCodecFactory factory = new CompressionCodecFactory(job);
			CompressionCodec codec = factory.getCodec(file);
				
			int id;
			IntArrayWritable array = new IntArrayWritable();
			
			//直接从HDFS中读取数据
			InputStream in = null;
			try
			{
				System.out.println("building " + file.toUri());
				FileSystem fs = FileSystem.get(file.toUri(), job);
				if (null == fs)
				{
					System.out.println("error happed");
					throw new IOException("error happed");
				}
				in = codec.createInputStream(fs.open(file));
				DataInput input = new DataInputStream(in);
				while (in.available() > 0)
				{
					id = input.readInt();
					array.readFields(input);
					aqsData[id] = array;
				}
			}
			catch (IOException ioe)
			{
				System.err.println("Caught exception while building data: "
								+ StringUtils.stringifyException(ioe));
			}
			finally
			{
				IOUtils.closeStream(in);
			}
			
			
			File f = new File(file.toString());
			long length = f.length();
			FileSplit split = new FileSplit(file, 0L, length, (String[])null);
			try
			{
				SequenceFileRecordReader<IntWritable, IntArrayWritable> reader = new SequenceFileRecordReader<IntWritable, IntArrayWritable>(job, split);
				
				IntWritable aid = reader.createKey();
				IntArrayWritable queries = reader.createValue();
				
				int count = 0;
				while (reader.getPos() < length)
				{
					reader.next(aid, queries);
					id = aid.get();
					aqsData[id] = queries;
					count++;
					if (count % 1000 == 0)
						System.out.println("building data: " + count);
				}
				reader.close();
				System.out.println("building data finished: " + count);
			}
			catch (IOException ioe)
			{
				System.err.println("Caught exception while building data: "
								+ StringUtils.stringifyException(ioe));
			}

			
			InputStream in = null;
			InputStream instream = null;
			try
			{
				in = new FileInputStream(file.toString());
				
				System.out.println("building " + file.toUri());
				//FileSystem fs = FileSystem.get(file.toUri(), conf);
				
				//instream = codec.createInputStream(in);
				//DataInput input = new DataInputStream(instream);
				DataInput input = new DataInputStream(in);
				id = input.readInt();
				array.readFields(input);
				aqsData[id] = array;
			}
			catch (IOException ioe)
			{
				System.err.println("Caught exception while building data: "
								+ StringUtils.stringifyException(ioe));
			}
			finally
			{
				IOUtils.closeStream(instream);
				IOUtils.closeStream(in);
			}
			
		}*/
		
		public void configure(JobConf job)
		{
			ad_num = job.getInt("EvidenceMatrixBuilder.ad_num", 0);
			inputAqsData = job.get("EvidenceMatrixBuilder.inputAqsData");
			aqsData = new IntArrayWritable[ad_num + 1];
			querySet = new QuerySetDigest();
			values = new ArrayList<Value>();			
			
			try
			{
//				Path[] files = DistributedCache.getLocalCacheFiles(job);
//				for (Path file : files)
//				{
//					System.out.println(file.toString());
//					buildData(file, job);
//				}
				FileSystem fs = FileSystem.get(job);
				Path inputPath = fs.makeQualified(new Path(inputAqsData));
				FileStatus[] files = fs.listStatus(inputPath);
				for (FileStatus fileStatus : files)
				{
					//buildData(fileStatus.getPath(), job);
					long length = fileStatus.getLen();
					FileSplit split = new FileSplit(fileStatus.getPath(), 0L, length, (String[])null);
					SequenceFileRecordReader<IntWritable, IntArrayWritable> reader = new SequenceFileRecordReader<IntWritable, IntArrayWritable>(job, split);
					
					IntWritable aid = reader.createKey();
					IntArrayWritable queries;
					
					//int count = 0;
					while (reader.getPos() < length)
					{
						queries = new IntArrayWritable();
						reader.next(aid, queries);
						int id = aid.get();
						if (id > ad_num)
						{
							System.err.println("ArrayIndexOutOfBoundsException: " + id + " of " + ad_num);
							continue;
						}
						aqsData[id] = queries;
					
//						count++;
//						if (count % 100000 == 0)
//							System.out.println("building " + fileStatus.getPath().getName() + " :" + count);
					}
					reader.close();
				}
			}
			catch (IOException ioe)
			{
				System.err.println("Caught exception while getting cached files: "
								+ StringUtils.stringifyException(ioe));
			}
		}

		/**
		 * 输入格式：qas ^A qid ^A { aid ^B click_num}
		 */
		@Override
		public void map(LongWritable lineOffset, Text line,
				OutputCollector<QuerySetDigest, Value> output, Reporter reporter)
				throws IOException
		{
			String strLine = line.toString();
			String[] elements = strLine.split(Common.CONTROL_A);
			int qid = Integer.parseInt(elements[1]);
			querySet.reset();
			querySet.addQueryID(qid);
			
			int aid;
			Value value;
			for (int i = 2; i < elements.length; i++)
			{
				value = new Value();
				value.id = qid;
				value.array.initialize(elements.length - 2);
				aid = Integer.parseInt(elements[i].substring(0, elements[i].indexOf(Common.CONTROL_B)));
				value.array.content[i - 2] = aid;
				values.add(value);
				
				//相关Query的集合{q(ir)}，q(i)-->{a}-->{q(ir)}
				IntArrayWritable rele_queries = aqsData[aid];
				if (null == rele_queries)
					continue;
				for (int q : rele_queries.content)
				{
					querySet.addQueryID(q);
				}
			}		

			querySet.generateDigest();
			for (Value v : values)
			{
				output.collect(querySet, v);
			}
		}
			
	}
	
	private static class JobReducer extends MapReduceBase
		implements Reducer<QuerySetDigest, Value, IndexPair, DoubleWritable>
	{
		private ArrayList<Value> values = new ArrayList<Value>();
		private IndexPair index = new IndexPair();
		private DoubleWritable result = new DoubleWritable();
		
		@Override
		public void reduce(QuerySetDigest key, Iterator<Value> valueList,
				OutputCollector<IndexPair, DoubleWritable> output, Reporter reporter)
				throws IOException
		{
			Value v;
			values.clear();
			while (valueList.hasNext())
			{
				v = valueList.next();
				if (!values.contains(v))	//防止加入重复元素
					values.add(v.clone());
			}
			
			int size = 0;
			Value value1, value2;
			int len = values.size();
			for (int i = 0; i < len; ++i)
			{
				for (int j = i + 1; j < len; ++j)
				{
					value1 = values.get(i);
					value2 = values.get(j);
					index.index1 = value1.id;
					index.index2 = value2.id;
					size = getIntersectionSize(value1.array, value2.array);
					if (size <= 0)//直接跳过，默认值为0.5
						continue;
					result.set(getEvidence(size));
					output.collect(index, result);
				}
			}
		}
		
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
	}
	
	protected static void configJob(JobConf conf)
	{
		conf.setJobName("Evidence Matrix Builder");
		conf.setJarByClass(EvidenceMatrixBuilder.class);	
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		conf.setMapperClass(JobMapper.class);
		conf.setReducerClass(JobReducer.class);
	
		conf.setMapOutputKeyClass(QuerySetDigest.class);
		conf.setMapOutputValueClass(Value.class);

		conf.setOutputKeyClass(IndexPair.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setInt("io.sort.mb", 256);
		conf.setInt("io.sort.factor", 100);
		conf.set("mapred.child.java.opts", "-Xmx3072m");
	}
	
	public static boolean runJob(String inputPathQas, String inputPathAqs, String outputDirPath, String tempDirPath, int R) throws Exception
	{
		if (inputPathQas == null || inputPathQas.isEmpty())
			throw new Exception("inputPathQas is null or empty");
		if (inputPathAqs == null || inputPathAqs.isEmpty())
			throw new Exception("inputPathAqs is null or empty");
		if (outputDirPath == null || outputDirPath.isEmpty())
			throw new Exception("outputDirPath is null or empty");		
		if (tempDirPath == null || tempDirPath.isEmpty())
			throw new Exception("tempDirPath is null or empty");
		
		//首先产生需要的格式的文件
//		if (!AQDataFormater.runJob(inputPathAqs, tempDirPath, 10))
//		{
//			System.err.println("Error happened in job Ad-Query Format Transformer");
//			return false;
//		}
		
		System.out.print("************************************************* <building evidence matrix> ");
		System.out.println("*************************************************");
		
		JobConf job = new JobConf();
		job.setInt("EvidenceMatrixBuilder.ad_num", 43304094);
		job.set("EvidenceMatrixBuilder.inputAqsData", tempDirPath);
		FileSystem fs = FileSystem.get(job);
		inputPathQas = fs.makeQualified(new Path(inputPathQas)).toString();
		//inputPathAqs = fs.makeQualified(new Path(inputPathAqs)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		//tempDirPath = fs.makeQualified(new Path(tempDirPath)).toString();
		//job.set("WeightMatrixBuilder.inputPathQas", inputPathQas);
		FileInputFormat.addInputPath(job, new Path(inputPathQas));
		//FileInputFormat.addInputPath(job, new Path(inputPathAqs));
		FileOutputFormat.setOutputPath(job,	new Path(outputDirPath));
		
//		//添加DistributedCache文件
//		FileStatus[] files = fs.listStatus(new Path(tempDirPath));
//		for (FileStatus fileStatus : files)
//		{
//			System.out.println(fileStatus.getPath().toUri());
//			DistributedCache.addCacheFile(fileStatus.getPath().toUri(), job);
//		}
		job.setNumMapTasks(50);
		job.setNumReduceTasks(R);		
		fs.delete(new Path(outputDirPath), true);
		
		try
		{
			configJob(job);
			RunningJob runningJob = JobClient.runJob(job);
			return runningJob.isSuccessful();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			//fs.delete(new Path(tempDirPath), true); //删除临时格式文件
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception
	{
		String INPUT_PATH_QAS = "/group/tbalgo-dev/zhouxj/aqr/20110829/qas/qaspart-00000";
		String INPUT_PATH_AQS = "/group/tbalgo-dev/zhouxj/aqr/20110829/aqs/";
		String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
		String TEMP_DIR = WORK_DIR + "/aqs_data";
		String OUTPUT_PATH_DIR = WORK_DIR + "/evidence";
		runJob(INPUT_PATH_QAS, INPUT_PATH_AQS, OUTPUT_PATH_DIR, TEMP_DIR, 200);
	}
}
