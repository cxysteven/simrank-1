package com.taobao.research.jobs.matrix_multiply;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.WeakHashMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import com.taobao.research.jobs.common.IndexPair;

/**
 * 矩阵乘法计算
 * 与前一版相比：
 * 1. 相似性分数的类型由 double -> float,以便节省IO开销、HDFS空间、Shuffle阶段的网络传输带宽
 * 2. 去掉二次排序的功能，Reduce节点自己负责对需要的数据进行排序,以便节省IO开销、HDFS空间、Shuffle阶段的网络传输带宽
 * 3. 优化Reduce节点，稀疏矩阵乘法算法
 * 4. 针对SimRank算法加入了如果结果矩阵是对称阵时，只输出下三角矩阵的逻辑
 * @author yangxudong.pt
 * @version 2.0
 * @deprecated
 */
public class MatrixMultiply2
{
	/**	True to print debugging messages. */
	private static final boolean DEBUG = false;
	
	/**	Global variables for the mapper and reducer tasks. */	
	private static String inputPathA;

	private static int strategy;
	private static int I;	//(转置后的)矩阵A的行数
	private static int K;	//(转置后的)矩阵A的列数、矩阵B的行数
	private static int J;	//矩阵B的列数
	private static int IB;	//(转置后的)矩阵A的分块子矩阵的行数
	private static int KB;	//(转置后的)矩阵A的分块子矩阵的列数、矩阵B的分块子矩阵的行数
	private static int JB;	//矩阵B的分块子矩阵的列数
	
	private static int NIB;	//矩阵A的行分块数
	private static int NKB;	//矩阵A的列分块数、矩阵B的行分块数
	private static int NJB;	//矩阵B的列分块数
	
	/**
	 * 是否启用Key的第四个元素，在strategy==1 或 4时，
	 * Key的第四个元素用来标识Value来自于矩阵A还是矩阵B
	 */
	private static boolean useM;
	/**
	 * 衰减因子，用于计算SimRank分数，默认值为1.0
	 * decay_factor的值不等于默认值1.0时，会激活SimRank算法的应用
	 * 即矩阵的乘法计算结果会额外地乘上衰减因子，同时对角线上的元素会被全部设为1.0
	 */
	private static float decay_factor;
	/** 过滤阈值, 把相似性分数低的值过滤掉，节省存储空间和计算量  */
	private static float threshold;
	/** 
	 * 是否是计算转置后的矩阵和转置前的同一矩阵相乘
	 * SimRank的主要算法：S(k+1) = C * trans(S(k) * P) * P
	 * S(0) = I （单位矩阵），因而S(1) = C * trans(P) * P
	 * SimRank算法第一轮迭代时，应把transpose_multiply_self设为true, 其余情况下设为false
	 */
	private static boolean transpose_multiply_self;
	/** 是否对A矩阵先进行转置，然后再做乘法 */
	private static boolean transposeA;
	
	private static boolean symmetrical_result;
	
	/**	The job 1 intermediate value class. */	
	/**据量的大小还在Integer能表示的范围内，可能以后要改成long型*/
	private static class Value implements WritableComparable<Value> {
		public int index1;
		public int index2;
		public float v;
		
		public Value(){}
		public Value(int i1, int i2, float v)
		{
			set(i1, i2, v);
		}
		public void set(int i1, int i2, float v)
		{
			index1 = i1;
			index2 = i2;
			this.v = v;
		}
		public void write (DataOutput out)
			throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			out.writeFloat(v);
		}
		public void readFields (DataInput in)
			throws IOException
		{
			index1 = in.readInt();
			index2 = in.readInt();
			v = in.readFloat();
		}
		@Override
		public Value clone()
		{	
			return new Value(index1, index2, v);	
		}
		@Override
		public boolean equals(Object o)
		{
			if (o instanceof Value)
			{
				Value r = (Value)o;
				if (index1 == r.index1 && index2 == r.index2 && v == r.v)
					return true;
			}
			return false;
		}
		@Override
		public int compareTo(Value o)
		{
			if (this.index1 != o.index1)
				return (this.index1 < o.index1) ? -1 : 1;
			
			if (this.index2 != o.index2)
				return (this.index2 < o.index2) ? -1 : 1;
			
			return this.v == o.v ?  0 : (this.v < o.v ? -1 : 1);
		}
	}
	
	/**	The job 1 raw intermediate key class. */
	private static class Key implements WritableComparable<Key> {
		public int index1;
		public int index2;
		public int index3;
		public byte m;
		public void write (DataOutput out) throws IOException
		{
			out.writeInt(index1);
			out.writeInt(index2);
			out.writeInt(index3);
			if (useM)
				out.writeByte(m);
		}
		public void readFields (DataInput in) throws IOException		
		{
			index1 = in.readInt();
			index2 = in.readInt();
			index3 = in.readInt();
			if (useM)
				m = in.readByte();
		}

		public int compareTo(Key o)
		{
			if (this.index1 != o.index1)
				return (this.index1 < o.index1) ? -1 : 1;

			if (this.index2 != o.index2)
				return (this.index2 < o.index2) ? -1 : 1;

			if (this.index3 != o.index3)
				return (this.index3 < o.index3) ? -1 : 1;

			/** 若在Map节点或Reduce节点启动之前调用这个方法，useM总是==false(默认值)
			 *  因而，在Shuffle & Sort阶段不能依靠useM的值来进行Key值的比较
			 *  这就是之前发现的Reduce端接受到的Key可能没有排好序的原因 */
			if (!useM)
				return 0;

			if (this.m != o.m)
				return (this.m < o.m) ? -1 : 1;

			return 0;
		}
		
		@Override
		public boolean equals(Object o)
		{
			if (this == o)
				return true;
			if (!(o instanceof Key))
				return false;
			Key k2 = (Key)o;
			if (this.index1 == k2.index1 && this.index2 == k2.index2 && this.index3 == k2.index3)
			{
				if (!useM)
					return true;
				
				return this.m == k2.m;
			}
			return false;
		}
		/** A Comparator that compares serialized Key. */ 
		public static class Comparator extends WritableComparator
		{
			public Comparator()
			{
				super(Key.class);
			}

			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int index11 = readInt(b1, s1);
				int index12 = readInt(b2, s2);
				if (index11 != index12)
					return (index11 < index12) ? -1 : 1;

				int index21 = readInt(b1, s1 + Integer.SIZE / 8);
				int index22 = readInt(b2, s2 + Integer.SIZE / 8);
				if (index21 != index22)
					return (index21 < index22) ? -1 : 1;

				int index31 = readInt(b1, s1 + Integer.SIZE * 2 / 8);
				int index32 = readInt(b2, s2 + Integer.SIZE * 2 / 8);
				if (index31 != index32)
					return (index31 < index32) ? -1 : 1;
				
				
				int len = Integer.SIZE * 3 / 8;			
				if (l1 > len)
				{//useM = true，但是不能用useM来判断，因为init(job)还没有被调用
					int start1 = s1 + len;
					int start2 = s2 + len;
					byte m1 = b1[start1];
					byte m2 = b2[start2];

					if (m1 != m2)
					{
						return (m1 < m2) ? -1 : 1;
					}
				}

				return 0;
			}
		}

		static
		{ // register this comparator
			WritableComparator.define(Key.class, new Comparator());
		}
	}
	
	/**	The job 1 mapper class. */
	private static class Job1Mapper 
		extends MapReduceBase implements Mapper<IndexPair, FloatWritable, Key, Value>
	{
		private Path path;
		private boolean matrixA;
		private Key key = new Key();
		private Value value = new Value();
		
		public void configure(JobConf job) {
			init(job);
			String inputPath = job.get("map.input.file");
			path = new Path(inputPath);
			matrixA = inputPath.indexOf(inputPathA) >= 0;
			if (DEBUG) {
				System.out.println("##### Map setup: matrixA = " + matrixA + " for " + inputPath);
				System.out.println("   strategy = " + strategy);
				System.out.println("   I = " + I);
				System.out.println("   K = " + K);
				System.out.println("   J = " + J);
				System.out.println("   IB = " + IB);
				System.out.println("   KB = " + KB);
				System.out.println("   JB = " + JB);
				System.out.println("   NIB = " + NIB);
				System.out.println("   NKB = " + NKB);
				System.out.println("   NJB = " + NJB);
				System.out.println("   decay factor = " + decay_factor);
			}
		}
		
		
		private void printMapInput (IndexPair indexPair, FloatWritable el) {
			System.out.println("##### Map input: (" + indexPair.index1 + "," + 
				indexPair.index2 + ") " + el.get());
		}
		
		private void printMapOutput (Key key, Value value) {
			System.out.println("##### Map output: (" + key.index1 + "," + 
				key.index2 + "," + key.index3 + "," + key.m + ") (" + 
				value.index1 + "," + value.index2 + "," + value.v + ") ");
		}
		
		private void badIndex (int index, int dim, String msg) throws IOException 
		{
			String strMsg = "Invalid " + msg + " in " + path + ": " + index + " " + dim;
			System.err.println(strMsg);
			//System.exit(1);	//直接exit,错误日志只有登录到目标机才能查看，因为改为抛异常
			throw new IOException(strMsg);
		}
		
		private void emitWithStrategy1(OutputCollector<Key, Value> output, int i, int k, int j) throws IOException
		{
			if (matrixA)
			{
				key.index1 = i / IB;
				key.index2 = k / KB;
				key.m = 1;
				value.index1 = i % IB;
				value.index2 = k % KB;
				for (int jb = 0; jb < NJB; jb++)
				{
					key.index3 = jb;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
			}
			else
			{
				key.index2 = k / KB;
				key.index3 = j / JB;
				key.m = 0;
				value.index1 = k % KB;
				value.index2 = j % JB;
				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
			}
		}
		
		private void emitWithStrategy2(OutputCollector<Key, Value> output, int i, int k, int j) throws IOException
		{
			if (matrixA)
			{
				key.index1 = i / IB;
				key.index2 = k / KB;
				key.index3 = -1;
				value.index1 = i % IB;
				value.index2 = k % KB;
				output.collect(key, value);
				if (DEBUG)
					printMapOutput(key, value);
			}
			else
			{
				key.index2 = k / KB;
				key.index3 = j / JB;
				value.index1 = k % KB;
				value.index2 = j % JB;
				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
			}
		}
		
		private void emitWithStrategy3(OutputCollector<Key, Value> output, int i, int k, int j) throws IOException
		{
			if (matrixA)
			{
				key.index1 = k / KB;
				key.index3 = i / IB;
				value.index1 = i % IB;
				value.index2 = k % KB;
				for (int jb = 0; jb < NJB; jb++)
				{
					key.index2 = jb;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
			}
			else
			{
				key.index1 = k / KB;
				key.index2 = j / JB;
				key.index3 = -1;
				value.index1 = k % KB;
				value.index2 = j % JB;
				output.collect(key, value);
				if (DEBUG)
					printMapOutput(key, value);
			}
		}
		
		private void emitWithStrategy4(OutputCollector<Key, Value> output, int i, int k, int j) throws IOException
		{
			if (matrixA)
			{
				key.index1 = i / IB;
				key.index3 = k / KB;
				key.m = 1;
				value.index1 = i % IB;
				value.index2 = k % KB;
				for (int jb = 0; jb < NJB; jb++)
				{
					key.index2 = jb;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
			}
			else
			{
				key.index2 = j / JB;
				key.index3 = k / KB;
				key.m = 0;
				value.index1 = k % KB;
				value.index2 = j % JB;
				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
			}
		}
		
		/** 计算转置后的矩阵和转置前的同一矩阵相乘 */
		public void map_transpose_multiply_self(IndexPair indexPair,
				FloatWritable el, OutputCollector<Key, Value> output,
				Reporter reporter) throws IOException
		{
			// 定义：A矩阵是转置后的输入矩阵，B矩阵是转置前的输入矩阵
			// A矩阵的索引
			int i = indexPair.index2;
			if (i < 0 || i >= I)
				badIndex(i, I, "A row index");
			int k = indexPair.index1;
			if (k < 0 || k >= K)
				badIndex(k, K, "A column index");

			// B矩阵的索引
			int k2 = indexPair.index1;
			int j = indexPair.index2;

			value.v = el.get();
			switch (strategy)
			{
			case 1:
				// 收集A矩阵的值
				key.index1 = i / IB;
				key.index2 = k / KB;
				key.m = 1;
				value.index1 = i % IB;
				value.index2 = k % KB;

				for (int jb = 0; jb < NJB; jb++)
				{
					key.index3 = jb;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
				// 收集B矩阵的值
				key.index2 = k2 / KB;
				key.index3 = j / JB;
				key.m = 0;
				value.index1 = k2 % KB;
				value.index2 = j % JB;

				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
				break;
			case 2:
				// 收集A矩阵的值
				key.index1 = i / IB;
				key.index2 = k / KB;
				key.index3 = -1;
				value.index1 = i % IB;
				value.index2 = k % KB;

				output.collect(key, value);
				if (DEBUG)
					printMapOutput(key, value);
				// 收集B矩阵的值
				key.index2 = k2 / KB;
				key.index3 = j / JB;
				value.index1 = k2 % KB;
				value.index2 = j % JB;

				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
				break;
			case 3:
				// 收集A矩阵的值
				key.index1 = k / KB;
				key.index3 = i / IB;
				value.index1 = i % IB;
				value.index2 = k % KB;

				for (int jb = 0; jb < NJB; jb++)
				{
					key.index2 = jb;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
				// 收集B矩阵的值
				key.index1 = k2 / KB;
				key.index2 = j / JB;
				key.index3 = -1;
				value.index1 = k2 % KB;
				value.index2 = j % JB;

				output.collect(key, value);
				if (DEBUG)
					printMapOutput(key, value);
				break;
			case 4:
				// 收集A矩阵的值
				key.index1 = i / IB;
				key.index3 = k / KB;
				key.m = 1;
				value.index1 = i % IB;
				value.index2 = k % KB;

				for (int jb = 0; jb < NJB; jb++)
				{
					key.index2 = jb;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
				// 收集B矩阵的值
				key.index2 = j / JB;
				key.index3 = k2 / KB;
				key.m = 0;
				value.index1 = k2 % KB;
				value.index2 = j % JB;

				for (int ib = 0; ib < NIB; ib++)
				{
					key.index1 = ib;
					output.collect(key, value);
					if (DEBUG)
						printMapOutput(key, value);
				}
				break;
			}
		}
		
		public void map(IndexPair indexPair, FloatWritable el,
				OutputCollector<Key, Value> output, Reporter reporter)
				throws IOException
		{
			if (DEBUG)
				printMapInput(indexPair, el);
			if (el.get() == 0.0)
				return;
			if (transpose_multiply_self)
			{
				map_transpose_multiply_self(indexPair, el, output, reporter);
				return;
			}

			int i = 0;
			int k = 0;
			int j = 0;
			if (matrixA)
			{
				if (transposeA)// 在计算矩阵前对A矩阵进行转置
				{
					i = indexPair.index2;
					if (i < 0 || i >= I)
						badIndex(i, I, "A row index");
					k = indexPair.index1;
					if (k < 0 || k >= K)
						badIndex(k, K, "A column index");
				}
				else
				{
					i = indexPair.index1;
					if (i < 0 || i >= I)
						badIndex(i, I, "A row index");
					k = indexPair.index2;
					if (k < 0 || k >= K)
						badIndex(k, K, "A column index");
				}
			}
			else
			{
				k = indexPair.index1;
				if (k < 0 || k >= K)
					badIndex(k, K, "B row index");
				j = indexPair.index2;
				if (j < 0 || j >= J)
					badIndex(j, J, "B column index");
			}
			value.v = el.get();
			switch (strategy)
			{
			case 1:
				emitWithStrategy1(output, i, k, j);
				break;
			case 2:
				emitWithStrategy2(output, i, k, j);
				break;
			case 3:
				emitWithStrategy3(output, i, k, j);
				break;
			case 4:
				emitWithStrategy4(output, i, k, j);
				break;
			}
			
			//SimRank算法特有的步骤，如仅仅测试矩阵乘法是否正确，请注释掉下面的if语句
			//如果计算结果不是对称矩阵，则输入矩阵的左操作数是一个对称矩阵（仅存储了一半的）
			if (matrixA && !symmetrical_result && i != k)
			{
				i = indexPair.index2;
				k = indexPair.index1;
				switch (strategy)
				{
				case 1:
					emitWithStrategy1(output, i, k, j);
					break;
				case 2:
					emitWithStrategy2(output, i, k, j);
					break;
				case 3:
					emitWithStrategy3(output, i, k, j);
					break;
				case 4:
					emitWithStrategy4(output, i, k, j);
					break;
				}
			}
		}
	}
	
	/**	The job 1 partitioner class. */
	private static class Job1Partitioner
		implements Partitioner<Key, Value>
	{
		public int getPartition (Key key, Value value, int numPartitions) {
			int kb, ib, jb;
			switch (strategy) 
			{
			case 1:
				ib = key.index1;
				kb = key.index2;
				jb = key.index3;
				return ((ib + jb * NIB) * NKB + kb) % numPartitions;
			case 2:
				ib = key.index1;
				kb = key.index2;
				return (ib*NKB + kb) % numPartitions;
			case 3:
				kb = key.index1;
				jb = key.index2;
				return (jb*NKB + kb) % numPartitions;
			case 4:
				ib = key.index1;
				jb = key.index2;
				return (ib*NJB + jb) % numPartitions;
			}
			return 0;
		}

		@Override
		public void configure(JobConf arg0)
		{}
	}
	
	/**	The job 1 reducer class. */
	public static class Job1Reducer
		extends MapReduceBase implements Reducer<Key, Value, IndexPair, FloatWritable>
	{
		//////////////////////////////////////////////
		//稀疏矩阵的乘法
		private ArrayList<Value> VA;		//存储A矩阵的连接表
		private ArrayList<Value> VB;		//存储B矩阵的连接表
		private Map<IndexPair, Float> VC;	//存储目标C矩阵的连接表
		private int[] bRowPos;				//存储B矩阵的行连接信息
		//private float[] cRow;				//存储C矩阵的一行
		//private TreeSet<Value> MB = new TreeSet<Value>();
		//////////////////////////////////////////////
		private int sib, skb, sjb;
		//private int aRowDim, aColDim, bColDim, bRowDim;
		private IndexPair indexPair = new IndexPair();
		private FloatWritable score = new FloatWritable();
		//用于strategy==4时，在close函数中收集数据
		private OutputCollector<IndexPair, FloatWritable> outputCollector;
		///////////////////////////////////////////////
		
		public void configure(JobConf job) {
			init(job);
			if (DEBUG) {
				System.out.println("##### Reduce setup");
				System.out.println("   strategy = " + strategy);
				System.out.println("   I = " + I);
				System.out.println("   K = " + K);
				System.out.println("   J = " + J);
				System.out.println("   IB = " + IB);
				System.out.println("   KB = " + KB);
				System.out.println("   JB = " + JB);
				System.out.println("   useM = " + useM);
				System.out.println("   symmetrical_result = " + symmetrical_result);
				System.out.println("   decay factor = " + decay_factor);
			}

			VB = new ArrayList<Value>(10 * KB);
			if (strategy == 2)
				VA = new ArrayList<Value>(10 * IB);
			if (strategy == 4)
			{
				int capacity = IB * JB / 1000;
				//int capacity = IB * JB;
				if (capacity < 32)
					capacity = 32;
				VC = new WeakHashMap<IndexPair, Float>(capacity);
			}
			bRowPos = new int[KB + 1];

			sib = -1;
			skb = -1;
			sjb = -1;
		}
		
		private void printReduceInputKey(Key key)
		{
			System.out.println("##### Reduce input: key = (" + key.index1 + "," + 
					key.index2 + "," + key.index3 + "," + key.m + ")");
		}
		
		private void printReduceInputValue (Value value) {
			System.out.println("##### Reduce input: value = (" + value.index1 + "," +
				value.index2 + "," + value.v + ")");
		}
		
		private void printReduceOutput () {
			System.out.println("##### Reduce output: (" + indexPair.index1 + "," + 
				indexPair.index2 + ") " + score.get());
		}
		

/*		private void build(Iterator<Value> valueList, TreeSet<Value> matrix, int[] rowPos)
		{
			if (null == matrix || !valueList.hasNext())
			{
				System.err.println("Error in build: <matrix is null> or <no value in valueList>");
				return;
			}
			matrix.clear();
			while (valueList.hasNext())
			{
				matrix.add(valueList.next().clone());
			}
			
			if (DEBUG)
			{
				Iterator<Value> iter = matrix.iterator();
				while (iter.hasNext())
				{
					printReduceInputValue(iter.next());
				}
			}
			
			if (null == rowPos)
				return;
			
			int lastPos = 0;//上一行在连接表中的位置
			int lastRow = 0;//上一行的行号
			Value value = matrix.first();
			for (int i = lastRow; i <= value.index1; ++i)
				rowPos[i] = lastPos;
			
			lastPos++;
			lastRow = value.index1;
			
			Iterator<Value> iter = matrix.iterator();
			while (iter.hasNext())
			{
				if (lastRow == value.index1)
					lastPos++;
				else
				{						
					for (int i = lastRow + 1; i <= value.index1; ++i)
						rowPos[i] = lastPos;
					
					lastPos++;
					lastRow = value.index1;
				}
			}
			for (int i = lastRow + 1; i <= KB; i++)
				rowPos[i] = lastPos;	//填充结尾的值，否则可能会出错
		}
*/		
		//存储矩阵，同时构建行连接信息，以便于矩阵乘法计算
		
		private void build(Iterator<Value> valueList, ArrayList<Value> matrix, int[] rowPos)
		{
			if (null == matrix || !valueList.hasNext())
			{
				System.err.println("Error in build: <matrix is null> or <no value in valueList>");
				return;
			}
			
			if (DEBUG)
			{
				if (matrix == VB)
					System.out.println("building VB");
				else if (matrix == VA)
					System.out.println("building VA");
			}
			
			//清空上次计算遗留下来的旧值
			matrix.clear();
			while (valueList.hasNext())
			{
				matrix.add(valueList.next().clone());
			}
			
			if (null == rowPos)
				return;
			//生成行位置信息
			Collections.sort(matrix); //先排序
			
			int lastPos = 0;//上一行在连接表中的位置
			int lastRow = 0;//上一行的行号
			
			//处理连接表中的第一行数据
			Value value = matrix.get(0);
			if (DEBUG)
				printReduceInputValue(value);

			for (int i = lastRow; i <= value.index1; ++i)
				rowPos[i] = lastPos;
			
			lastPos++;
			lastRow = value.index1;
			int size = matrix.size();
			for (int v = 1; v < size; ++v)
			{
				value = matrix.get(v);
				if (DEBUG)
					printReduceInputValue(value);

				if (lastRow == value.index1)
					lastPos++;
				else
				{						
					for (int i = lastRow + 1; i <= value.index1; ++i)
						rowPos[i] = lastPos;
					
					lastPos++;
					lastRow = value.index1;
				}			
			}
			if (null != rowPos)
			{
				for (int i = lastRow + 1; i <= KB; i++)
					rowPos[i] = lastPos;	//填充结尾的值，否则可能会出错
				//rowPos[lastRow + 1] = lastPos; //指向结尾
			}
			
			if (DEBUG)
			{
				System.out.print("rowPos: ");
				for (int i = 0; i <= KB; i++)
					System.out.print(rowPos[i] + " ");
				System.out.println();
			}
		}
		
		//矩阵A和矩阵B相乘，矩阵A的值由Iterator<Value> values提供
		private void multiplyAndEmit(OutputCollector<IndexPair, FloatWritable> output, Iterator<Value> values, int ib, int jb)
				throws IOException
		{
			if (VB.isEmpty() || !values.hasNext())
				return;
			
			int ibase = ib*IB;
			int jbase = jb*JB;
			
			int start, last, bRow;
			Value aValue, bValue;
			while (values.hasNext())
			{
				aValue = values.next();
				if (DEBUG) printReduceInputValue(aValue);
				bRow = aValue.index2;
				start = bRowPos[bRow];
				last = bRowPos[bRow + 1];
				
				for (int i = start; i < last; ++i)
				{
					bValue = VB.get(i);
					float value = aValue.v * bValue.v * decay_factor;
					if (value < threshold) //阈值过滤
						continue;
					
					indexPair.index1 = ibase + aValue.index1;
					indexPair.index2 = jbase + bValue.index2;
					
					if (symmetrical_result)
					{
						//仅存储对称矩阵的下三角阵
						//在for循环内部indexPair.index1值不变，由于VB的排好序的indexPair.index2单调增
						if (indexPair.index1 < indexPair.index2)
							break;
					}

					score.set(value);
					//由job2来负责把对角线上的元素设为1
					output.collect(indexPair, score);
					if (DEBUG) printReduceOutput();
				}
			}
		}
			
		//矩阵A和矩阵B相乘，矩阵A的值预先存储在VA中
		private void multiplyAndEmit (OutputCollector<IndexPair, FloatWritable> output, int ib, int jb)
			throws IOException, InterruptedException 
		{
			if (VA.isEmpty() || VB.isEmpty())
				return;
			
			int ibase = ib*IB;
			int jbase = jb*JB;
			
			int start, last, bRow;
			Value bValue;
			for (Value aValue : VA)
			{
				if (DEBUG) printReduceInputValue(aValue);
				bRow = aValue.index2;
				start = bRowPos[bRow];
				last = bRowPos[bRow + 1];
				
				for (int i = start; i < last; ++i)
				{
					bValue = VB.get(i);
					float value = aValue.v * bValue.v * decay_factor;
					if (value < threshold) //阈值过滤
						continue;
					indexPair.index1 = ibase + aValue.index1;
					indexPair.index2 = jbase + bValue.index2;
					
					if (symmetrical_result)
					{
						//仅存储对称矩阵的下三角阵
						//在for循环内部indexPair.index1值不变，由于VB的排好序的indexPair.index2单调增
						if (indexPair.index1 < indexPair.index2)
							break;
					}
								
					score.set(value);
					
					//正在进行SimRank算法计算
//					if (indexPair.index1 == indexPair.index2 && decay_factor < 1.0)
//					{//对角线上的元素设为1
//						score.set(1.0f);
//						output.collect(indexPair, score);
//						if (DEBUG) printReduceOutput();
//					}
//					else
//					{
						output.collect(indexPair, score);
						if (DEBUG) printReduceOutput();
//					}
				}
			}
		}
		
		//矩阵A和矩阵B相乘，并把结果累积到相应的C矩阵中，矩阵A的值由Iterator<Value> values提供
		private void multiplyAndSum (Iterator<Value> values) {
			if (VB.isEmpty() || !values.hasNext())
				return;
			
			int start, last, bRow;
			Value aValue, bValue;
			while (values.hasNext())
			{
				aValue = values.next();
				//if (DEBUG) printReduceInputValue(aValue);
				bRow = aValue.index2;
				start = bRowPos[bRow];
				last = bRowPos[bRow + 1];
				
				for (int i = start; i < last; ++i)
				{
					bValue = VB.get(i);	
					IndexPair index = new IndexPair();
					index.index1 = aValue.index1;
					index.index2 = bValue.index2;
		
					Float v = aValue.v * bValue.v;
					Float tmp = VC.get(index);
					if (null != tmp)
						VC.put(index, tmp + v);
					else
						VC.put(index, v);
				}
			}
		}
		
		
		//收集strategy 4的最终结果
		private void emit (OutputCollector<IndexPair, FloatWritable> output, int ib, int jb)
			throws IOException, InterruptedException 
		{
			int ibase = ib * IB;
			int jbase = jb * JB;
			float value;
			Map.Entry<IndexPair, Float> e;
			for (Iterator<Entry<IndexPair, Float>> it = VC.entrySet().iterator(); it.hasNext(); )
			{
				e = (Map.Entry<IndexPair, Float>)it.next();
				indexPair = e.getKey();
				indexPair.index1 += ibase;
				indexPair.index2 += jbase;
				value = e.getValue();
				if (decay_factor == 1.0d)
				{//普通矩阵计算
					if (value > threshold)//阈值过滤
					{
						score.set(value);
						output.collect(indexPair, score);
						if (DEBUG) printReduceOutput();
					}
					continue;
				}
				
				//正在进行SimRank算法计算
				if (indexPair.index1 == indexPair.index2)
				{//对角线上的元素设为1
					score.set(1.0f);
					output.collect(indexPair, score);
					if (DEBUG) printReduceOutput();
				}
				else if (value > threshold)//阈值过滤
				{
					if (symmetrical_result)
					{
						//仅存储对称矩阵的下三角阵
						//在for循环内部indexPair.index1值不变，由于VB的排好序的indexPair.index2单调增
						if (indexPair.index1 < indexPair.index2)
							continue;
					}
					score.set(value * decay_factor); //乘上衰减因子
					output.collect(indexPair, score);
					if (DEBUG) printReduceOutput();
				}
			}
			
			//清空C矩阵
			VC.clear();
		}
			
		@Override
		public void reduce(Key key, Iterator<Value> valueList,
				OutputCollector<IndexPair, FloatWritable> output,
				Reporter reporter) throws IOException
		{
			if (DEBUG)
				printReduceInputKey(key);

			int ib, kb, jb;
			switch (strategy)
			{
			case 1:
				ib = key.index1;
				kb = key.index2;
				jb = key.index3;
				if (key.m == 0)// B矩阵
				{
					sjb = jb;
					skb = kb;
					build(valueList, VB, bRowPos);
				}
				else
				{
					if (jb != sjb || kb != skb)
						return;

					try
					{
						multiplyAndEmit(output, valueList, ib, jb);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				}
				break;
			case 2:
				ib = key.index1;
				kb = key.index2;
				jb = key.index3;
				if (jb < 0)
				{
					sib = ib;
					skb = kb;
					build(valueList, VA, null);
				}
				else
				{
					if (ib != sib || kb != skb)
						return;

					try
					{
						build(valueList, VB, bRowPos);
						multiplyAndEmit(output, ib, jb);
					} catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
				break;
			case 3:
				kb = key.index1;
				jb = key.index2;
				ib = key.index3;
				if (ib < 0)
				{
					skb = kb;
					sjb = jb;
					//build(valueList, MB);
					build(valueList, VB, bRowPos);
				}
				else
				{
					if (kb != skb || jb != sjb)
						return;

					try
					{
						multiplyAndEmit(output, valueList, ib, jb);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				}
				break;
			case 4:
				ib = key.index1;
				jb = key.index2;
				kb = key.index3;
				outputCollector = output;
				if (ib != sib || jb != sjb)
				{
					if (sib != -1)
					{//收集上一个目标子矩阵的值
						try
						{
							emit(output, sib, sjb);
						} catch (InterruptedException e)
						{
							e.printStackTrace();
						}
					}
					sib = ib;
					sjb = jb;
					skb = -1;
				}
				if (key.m == 0)// B矩阵
				{				
					skb = kb;
					build(valueList, VB, bRowPos);
				}
				else
				{// A矩阵
					if (kb != skb)
						return;				

					multiplyAndSum(valueList);
				}
				break;
			}
		}
		
		// At the end of the reducer task we must emit the last C block
		@Override
		public void close()
		{
			if (strategy == 4 && sib != -1)
			{
				try
				{
					emit(outputCollector, sib, sjb);
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		
	}
	
	
	/**	Initializes the global variables from the job context for the mapper and reducer tasks. */	
	private static void init (JobConf conf) {		
		inputPathA = conf.get("MatrixMultiply.inputPathA");
		strategy = conf.getInt("MatrixMultiply.strategy", 0);
		I = conf.getInt("MatrixMultiply.I", 0);
		K = conf.getInt("MatrixMultiply.K", 0);
		J = conf.getInt("MatrixMultiply.J", 0);
		IB = conf.getInt("MatrixMultiply.IB", 0);
		KB = conf.getInt("MatrixMultiply.KB", 0);
		JB = conf.getInt("MatrixMultiply.JB", 0);
		threshold = conf.getFloat("MatrixMultiply.threshold", 0f);
		decay_factor = conf.getFloat("MatrixMultiply.decay_factor", 1.0f);
		transpose_multiply_self = conf.getBoolean("MatrixMultiply.transpose_multiply_self", false);
		/** 是否对A矩阵先进行转置，然后再做乘法 */
		transposeA = conf.getBoolean("MatrixMultiply.transposeA", false);
		symmetrical_result = conf.getBoolean("symmetrical_result", false);
		NIB = (I-1)/IB + 1;
		NKB = (K-1)/KB + 1;
		NJB = (J-1)/JB + 1;
		useM = strategy == 1 || strategy == 4;
	}
	

	public static class FloatSumReducer extends MapReduceBase implements
			Reducer<IndexPair, FloatWritable, IndexPair, FloatWritable>
	{
		private FloatWritable result = new FloatWritable();
		
		@Override
		public void configure(JobConf job)
		{
			decay_factor = job.getFloat("MatrixMultiply.decay_factor", 1.0f);
		}

		@Override
		public void reduce(IndexPair key, Iterator<FloatWritable> values,
				OutputCollector<IndexPair, FloatWritable> output, Reporter reporter)
				throws IOException
		{
			//通过decay_factor是否小于0来判断是否需要把对角线元素设为1
			if (decay_factor < 1.0 && key.index1 == key.index2)
			{
				result.set(1.0f);
				output.collect(key, result);
				return;
			}
			
			double sum = 0.0;
			while (values.hasNext())
			{
				sum += values.next().get();
			}
			result.set((float) sum);
			output.collect(key, result);
		}

	}
	
	/**	Configures and runs job 1. */	
	protected static boolean configJob1(Configuration conf)
	{
		int R = conf.getInt("MatrixMultiply.R1", 0);
		int MR = 0; //存储最多需要的Reduce数
		I = conf.getInt("MatrixMultiply.I", 0);
		K = conf.getInt("MatrixMultiply.K", 0);
		J = conf.getInt("MatrixMultiply.J", 0);
		IB = conf.getInt("MatrixMultiply.IB", 0);
		KB = conf.getInt("MatrixMultiply.KB", 0);
		JB = conf.getInt("MatrixMultiply.JB", 0);
		NIB = (I-1)/IB + 1;
		NKB = (K-1)/KB + 1;
		NJB = (J-1)/JB + 1;
		switch (conf.getInt("MatrixMultiply.strategy", 0))
		{
		case 1:
			MR = NIB * NKB * NJB;
			break;
		case 2:
			MR = NIB * NKB;
			break;
		case 3:
			MR = NKB * NJB;
			break;
		case 4:
			MR = NIB * NJB;
			break;
		}
		if (0 == R || R > MR)
		{//当R1设为0，或者R1过大时，自动调整reduce数
			R = MR;
		}
		JobConf job = new JobConf(conf, MatrixMultiply.class);
		job.setJobName("Matrix Multiply Job 1");
		job.setJarByClass(MatrixMultiply.class);	
		job.setNumReduceTasks(R);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setMapperClass(Job1Mapper.class);
		job.setReducerClass(Job1Reducer.class);
		job.setPartitionerClass(Job1Partitioner.class);		
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(Value.class);
		job.setOutputKeyClass(IndexPair.class);
		job.setOutputValueClass(FloatWritable.class);
		//对输出进行压缩
		job.setBoolean("mapred.output.compress", true);
		job.setClass("mapred.output.compression.codec", BZip2Codec.class, CompressionCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.inputPathA")));
		if (!conf.getBoolean("MatrixMultiply.transpose_multiply_self", false))
		{
			System.out.println(conf.get("MatrixMultiply.inputPathB"));
			FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.inputPathB")));
		}
		if (conf.getInt("MatrixMultiply.strategy", 0) == 4)
		{
			FileOutputFormat.setOutputPath(job,	new Path(conf.get("MatrixMultiply.outputDirPath")));
		}
		else
		{
			FileOutputFormat.setOutputPath(job,	new Path(conf.get("MatrixMultiply.tempDirPath")));
		}

		try
		{
			RunningJob runningJob = JobClient.runJob(job);
			return runningJob.isSuccessful();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	/**	Configures and runs job 2. */
	protected static boolean configJob2(Configuration conf)	
	{
		JobConf job = new JobConf(conf, MatrixMultiply.class);
		job.setJobName("Matrix Multiply Job 2");
		job.setJarByClass(MatrixMultiply.class);
		job.setNumReduceTasks(conf.getInt("MatrixMultiply.R2", 0));
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setMapperClass(IdentityMapper.class);
		job.setCombinerClass(FloatSumReducer.class);
		job.setReducerClass(FloatSumReducer.class);
		job.setOutputKeyClass(IndexPair.class);
		job.setOutputValueClass(FloatWritable.class);		
		FileInputFormat.addInputPath(job, new Path(conf.get("MatrixMultiply.tempDirPath")));
		FileOutputFormat.setOutputPath(job, new Path(conf.get("MatrixMultiply.outputDirPath")));
		//对输出进行压缩
		conf.setBoolean("mapred.output.compress", true);
		conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		try
		{
			RunningJob runningJob = JobClient.runJob(job);
			return runningJob.isSuccessful();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}
	
	/**	Runs a matrix multiplication job.
	 *
	 *	<p>This method is thread safe, so it can be used to run multiple concurrent
	 *	matrix multiplication jobs, provided each concurrent invocation uses a separate
	 *	configuration.
	 *
	 *	<p>The input and output files are sequence files, with key class 
	 *	MatrixMultiply.IndexPair and value class DoubleWritable.
	 *
	 *	@param	conf			The configuration.
	 *
	 *	@param	inputPathA		Path to input file or directory of input files for matrix A.
	 *
	 *	@param	inputPathB		Path to input file or directory of input files for matrix B.
	 *
	 *	@param	outputDirPath	Path to directory of output files for C = A*B. This directory 
	 *							is deleted if it already exists.
	 *
	 *	@param	tempDirPath		Path to directory for temporary files. A subdirectory
	 *							of this directory named "MatrixMultiply-nnnnn" is created
	 *							to hold the files that are the output of job 1 and the
	 *							input of job 2, where "nnnnn" is a random number. This 
	 *							subdirectory is deleted before the method returns. This
	 *							argument is only used for strategies 1, 2, and 3. Strategy
	 *							4 does not use a second job.
	 *
	 *	@param	strategy		The strategy: 1, 2, 3 or 4.
	 *
	 *	@param	R1				Number of reduce tasks for job 1.
	 *
	 *	@param	R2				Number of reduce tasks for job 2. Only used for strategies
	 *							1, 2, and 3.
	 *
	 *	@param	I				Row dimension of matrix A and matrix C.
	 *
	 *	@param	K				Column dimension of matrix A and row dimension of matrix B.
	 *
	 *	@param	J				Column dimension of matrix A and matrix C.
	 *
	 *	@param	IB				Number of rows per A block and C block.
	 *
	 *	@param	KB				Number of columns per A block and rows per B block.
	 *
	 *	@param	JB				Number of columns per B block and C block.
	 *
	 *  @param  threshold		threshold of the SimRank score
	 *  
	 *  @param  decayFactor		decay factor of SimRank algorithm
	 *  
	 *  @param  transA			whether matrix A need to be transposed
	 *  
	 *  @param  trans_multiply_self		whether to compute a transposed matrix multiply itself
	 *  
	 *  @param  symm_result whether the resultant matrix is symmetrical
	 *
	 *	@throws	Exception
	 */
	public static boolean runJob (Configuration conf, String inputPathA, String inputPathB,
		String outputDirPath, String tempDirPath, int strategy, int R1, int R2,
		int I, int K, int J, int IB, int KB, int JB, float threshold, float decayFactor,
		boolean transA, boolean trans_multiply_self, boolean symm_result)
			throws Exception
	{
		if (conf == null) throw new Exception("conf is null");
		if (inputPathA == null || inputPathA.length() == 0)
			throw new Exception("inputPathA is null or empty");
		//矩阵的转置和自身相乘时，可以不用输入B矩阵
		if ((inputPathB == null || inputPathB.length() == 0) && trans_multiply_self == false)
			throw new Exception("inputPathB is null or empty");
		if (outputDirPath == null || outputDirPath.length() == 0)
			throw new Exception("outputDirPath is null or empty");
		if (tempDirPath == null || tempDirPath.length() == 0)
			throw new Exception("tempDirPath is null or empty");
		if (strategy < 1 || strategy > 4)
			throw new Exception("strategy must be 1, 2, 3 or 4");
		if (R1 < -1) throw new Exception ("R1 must be >= -1");
		if (R2 < -1) throw new Exception ("R2 must be >= -1");
		if (I < 1) throw new Exception ("I must be >= 1");
		if (K < 1) throw new Exception ("K must be >= 1");
		if (J < 1) throw new Exception ("J must be >= 1");
		if (IB < 1 || IB > I) throw new Exception ("IB must be >= 1 and <= I");
		if (KB < 1 || KB > K) throw new Exception ("KB must be >= 1 and <= K");
		if (JB < 1 || JB > J) throw new Exception ("JB must be >= 1 and <= J");
		
		FileSystem fs = FileSystem.get(conf);
		inputPathA = fs.makeQualified(new Path(inputPathA)).toString();
		//矩阵的转置和自身相乘时，可以不用输入B矩阵
		if (null != inputPathB && !inputPathB.isEmpty())
			inputPathB = fs.makeQualified(new Path(inputPathB)).toString();
		outputDirPath = fs.makeQualified(new Path(outputDirPath)).toString();
		tempDirPath = fs.makeQualified(new Path(tempDirPath)).toString();
		tempDirPath = tempDirPath + "/MatrixMultiply-" +
        	Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
        	
		conf.set("MatrixMultiply.inputPathA", inputPathA);
		if (null != inputPathB && !inputPathB.isEmpty())
			conf.set("MatrixMultiply.inputPathB", inputPathB);
		conf.set("MatrixMultiply.outputDirPath", outputDirPath);
		conf.set("MatrixMultiply.tempDirPath", tempDirPath);
		conf.setInt("MatrixMultiply.strategy", strategy);
		conf.setInt("MatrixMultiply.R1", R1);
		conf.setInt("MatrixMultiply.R2", R2);
		conf.setInt("MatrixMultiply.I", I);
		conf.setInt("MatrixMultiply.K", K);
		conf.setInt("MatrixMultiply.J", J);
		conf.setInt("MatrixMultiply.IB", IB);
		conf.setInt("MatrixMultiply.KB", KB);
		conf.setInt("MatrixMultiply.JB", JB);
		conf.setBoolean("MatrixMultiply.transpose_multiply_self", trans_multiply_self);
		conf.setBoolean("MatrixMultiply.transposeA", transA);
		conf.setBoolean("symmetrical_result", symm_result);
		conf.set("MatrixMultiply.threshold", String.valueOf(threshold));
		conf.set("MatrixMultiply.decay_factor", String.valueOf(decayFactor));
		conf.set("mapred.child.java.opts", "-Xmx3072m");
		conf.setInt("io.sort.mb", 1024);
		conf.setInt("io.sort.factor", 200);
		conf.setInt("mapred.reduce.parallel.copies", 10);
		
		if (DEBUG)
		{
			System.out.println("MatrixMultiply.inputPathA: " + inputPathA);
			System.out.println("MatrixMultiply.inputPathB: " + inputPathB);
			System.out.println("MatrixMultiply.outputDirPath: " + outputDirPath);
			System.out.println("MatrixMultiply.tempDirPath: " + tempDirPath);
			System.out.println("MatrixMultiply.strategy: " + strategy);
			System.out.println("MatrixMultiply.R1: " + R1);
			System.out.println("MatrixMultiply.R2: " + R2);
			System.out.println("MatrixMultiply.I: " + I);
			System.out.println("MatrixMultiply.K: " + K);
			System.out.println("MatrixMultiply.J: " + J);
			System.out.println("MatrixMultiply.IB: " + IB);
			System.out.println("MatrixMultiply.KB: " + KB);
			System.out.println("MatrixMultiply.JB: " + JB);
			System.out.println("symmetrical_result: " + symm_result);
			System.out.println("MatrixMultiply.threshold: " + threshold);
			System.out.println("MatrixMultiply.decay_factor: " + decayFactor);			
			System.out.println("MatrixMultiply.transposeA: " + transA);
			System.out.println("MatrixMultiply.transpose_multiply_self: " + trans_multiply_self);
		}
		
		fs.delete(new Path(tempDirPath), true);
		fs.delete(new Path(outputDirPath), true);
		
		boolean bRet;
		try {
			System.out.print("-------------------------------------------------------");
			System.out.print("<Matrix Multiply Job1>");
			System.out.println("-------------------------------------------------------");
			bRet = configJob1(conf);
			if (!bRet)
				return bRet;
			if (strategy != 4)
			{
				System.out.print("-------------------------------------------------------");
				System.out.print("<Matrix Multiply Job2>");
				System.out.println("-------------------------------------------------------");
				bRet = configJob2(conf);
			}
		} finally {
			fs.delete(new Path(tempDirPath), true);
		}
		return bRet;
	}
}
