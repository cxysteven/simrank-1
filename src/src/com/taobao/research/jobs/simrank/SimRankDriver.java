package com.taobao.research.jobs.simrank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.matrix_multiply.MatrixMultiply3;
import com.taobao.research.jobs.matrix_multiply.MatrixMultiplyWithShortKey;

/**
 * SimRank is an iterative PageRank-like method for computing similarity. It
 * goes beyond direct cocitation for computing similarity much as PageRank goes
 * beyond direct linking for computing importance.
 * 
 * @author Yang Xudong
 */
public class SimRankDriver extends Configured implements Tool
{
	private static boolean DEBUG = false;
	/** The value for the SimRank decay factor */
	private double decay_factor = 0.6;
	/** 相似性分数的阈值，在计算过程中，小于阈值的相似性分数会被设过滤掉；如不需要过滤，把该值设为0 */
	private double threshold_s = 0.001;
	/** 抽取权值矩阵时，点击次数过滤的阈值 */
	private int threshold_c = 0;
	/** 算法的迭代总次数 */
	private int iter_times = 3;
	/** 算法当前的迭代布 */
	private int curr_iter = 0;
	/** 计算矩阵乘法时用的策略 */
	private int strategy = 3;	//策略3所需的IO最少
	/** 存储Query和广告的点击关系 */
	private String inputPath_QAS;
	/** 存储广告和Query的点击关系 */
	private String inputPath_AQS;
	/** 工作目录 */
	private String inputPath_WorkDir;
	/** 存储Query-Ad的转移矩阵（在（带权值的）连接矩阵的基础上，按列归一化）*/
	private String inputPath_W_Q2A;
	/** 存储Ad-Query的转移矩阵（在（带权值的）连接矩阵的基础上，按列归一化）*/
	private String inputPath_W_A2Q;
	/** 存储Query-Ad的证据矩阵 */
	//private String inputPath_E_Q2A;
	/** 存储Ad-Query的证据矩阵 */
	//private String inputPath_E_A2Q;
	/** 存储上一轮迭代(或第一轮迭代)的Q2Q分数矩阵 */
	private String outputPath_S_Q2Q;
	/** 存储该轮迭代的Q2Q分数矩阵
	//（计算A2A分数时依赖于上一轮的Q2Q分数结果，所以该轮迭代的结果不能直接放在outputPath_S_Q2Q中,
	//防止路径覆盖）<串行版本可以不用: in-place技术，加速收敛> */
	private String outputPath_S_Q2Q2;
	/** 存储该轮迭代的A2A分数矩阵 */
	private String outputPath_S_A2A;
	/** 存储Q2Q分数计算过程的中间结果 */
	private String outputPath_M_Q2Q;
	/** 存储A2A分数计算过程的中间结果 */
	private String outputPath_M_A2A;
	/** 存储矩阵计算过程中产生的中间结果 */
	private String tempPath;
	/** 计算矩阵乘法的hadoop job1所用的reduce数，若设为0，算法会自己计算最优的值 */
	private int MM_R1 = -1;	//默认值-1表示系统自动设置
	/** 计算矩阵乘法的hadoop job2所用的reduce数, 策略4不需要job2 */
	private int MM_R2 = -1;
	/** Query总数 */
	private int num_queries = 0;
	/** Ad总数 */
	private int num_ads = 0;
	/** 如矩阵计算的时需要转置A矩阵，则IB和KB表示转置之后的分块子矩阵的行数和列数 */
	/** 矩阵乘法运算的A矩阵（左运算数）的分块子矩阵的行数 */
	private int IB;
	/** 矩阵乘法运算的A矩阵（左运算数）的分块子矩阵的列数、B矩阵（右运算数）的分块子矩阵的列数 */
	private int KB;
	/** 矩阵乘法运算的B矩阵（右运算数）的分块子矩阵的列数 */
	private int JB;
	/** 初始时，是否根据已经计算出的Query之间的相似性分数计算Ad之间的相似性分数
	 *  inplace设为true时可以不用保存上一轮的Q2Q分数结果（即不需要使用outputPath_S_Q2Q2）
	 *  可以节省磁盘空间和加快收敛速度 */
	private boolean inplace = true;
	/** 矩阵乘法的任务是否使用short类型的Key */
	private boolean shortKey = true;
	
	private Configuration conf = new Configuration();
	private FileSystem fs;
	
	private void fillMatrix(double[][] matrix, Path path)
			throws IOException
	{
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		IndexPair indexPair = new IndexPair();
		//FloatWritable el = new FloatWritable();
		DoubleWritable el = new DoubleWritable();
		while (reader.next(indexPair, el))
		{
			matrix[indexPair.index1][indexPair.index2] = el.get();
		}
		reader.close();
	}

	public double[][] readMatrix(int rowDim, int colDim, String pathStr)
			throws IOException
	{
		Path path = new Path(pathStr);
		double[][] result = new double[rowDim][colDim];
		for (int i = 0; i < rowDim; i++)
			for (int j = 0; j < colDim; j++)
				result[i][j] = 0;
		if (fs.isFile(path))
		{
			fillMatrix(result, path);
		}
		else
		{
			FileStatus[] fileStatusArray = fs.listStatus(path);
			for (FileStatus fileStatus : fileStatusArray)
			{
				fillMatrix(result, fileStatus.getPath());
			}
		}
		return result;
	}
	
	public void run()
	{
		if (inplace)
			outputPath_S_Q2Q2 = outputPath_S_Q2Q;

		try
		{
			while (curr_iter < iter_times)
			{
				if (inplace)
				{
					if (curr_iter > 0)// 第一轮不计算Query之间的相似性
						computeQ2Q();
					if (curr_iter < iter_times - 1)// 最后一轮不计算广告之间的相似性
						computeA2A();
				}
				else
				{
					computeQ2Q();
					computeA2A();
				}
				curr_iter++;
			}
		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	/** 
	 * 计算Query-Query之间的相似性分数矩阵
	 * 计算公式：Q2Q(k+1) = C * transpose(A2A(k) * A2Q) * A2Q
	 * 其中，A2A(k)表示第k轮迭代时Ad-Ad之间的相似性分数矩阵
	 * A2Q是Ad-Query的转移矩阵（在（带权值的）连接矩阵的基础上，按列归一化）
	 * transpose(A)表示矩阵A的转置矩阵
	 * @return 是否执行成功
	 * @throws Exception
	 */
	public boolean computeQ2Q() throws Exception
	{
		System.out.print("*************************************************<computing Q2Q score. ");
		System.out.println("Iterate " + (curr_iter + 1) + " of " + iter_times + ">**********************************");

		boolean success;
		String inputPathA;
		String inputPathB;
		String outputPath;
		//第一轮迭代，求Q-Q的相似性
		if (0 == curr_iter)
		{
			inputPathA = inputPath_W_A2Q;
			inputPathB = null;
			outputPath = outputPath_S_Q2Q;
			
			if (shortKey)
			{
				success = MatrixMultiplyWithShortKey.runJob(conf, inputPathA, inputPathB,
					outputPath,	tempPath, strategy, MM_R1, MM_R2, num_queries, num_ads, num_queries, IB, KB, JB,
					threshold_s, decay_factor, true, true, true);
			}
			else
			{
				success = MatrixMultiply3.runJob(conf, inputPathA, inputPathB,
					outputPath,	tempPath, strategy, MM_R1, MM_R2, num_queries, num_ads, num_queries, IB, KB, JB,
					threshold_s, decay_factor, true, true, true);
			}
			
			//for debug
			if (DEBUG)
			{
				double[][] q_score = readMatrix(num_queries, num_queries, outputPath);			
				for (int i = 0; i < num_queries; i++)
				{
					for (int j = 0; j < num_queries; j++)
						System.out.print("Q[" + i + "][" + j + "]=" + q_score[i][j] + " ");
					System.out.println();
				}
			}
			
			return success;
		}
		else
		{
			//交换outputPath_S_Q2Q和outputPath_S_Q2Q2的内容
			if (!inplace && curr_iter > 1)
			{
				String temp = outputPath_S_Q2Q;
				outputPath_S_Q2Q = outputPath_S_Q2Q2;
				outputPath_S_Q2Q2 = temp;
			}
			
			inputPathA = outputPath_S_A2A;
			inputPathB = inputPath_W_A2Q;
			outputPath = outputPath_M_Q2Q;
			if (shortKey)
			{
				success = MatrixMultiplyWithShortKey.runJob(conf, inputPathA, inputPathB,
					outputPath, tempPath, strategy, MM_R1, MM_R2, num_ads, num_ads, num_queries, IB, KB, JB,
				threshold_s, 1.0f, false, false, false);
			}
			else
			{
				success = MatrixMultiply3.runJob(conf, inputPathA, inputPathB,
						outputPath, tempPath, strategy, MM_R1, MM_R2, num_ads, num_ads, num_queries, IB, KB, JB,
					threshold_s, 1.0f, false, false, false);			
			}
			
			//for debug
//			if (debug)
//			{
//				double[][] q_t_score = readMatrix(num_ads, num_queries, outputPath);
//				for (int i = 0; i < num_ads; i++)
//				{
//					for (int j = 0; j < num_queries; j++)
//						System.out.println("QT[" + i + "][" + j + "]=" + q_t_score[i][j]);
//				}
//			}
//			int R2 = MM_R2;
//			if (curr_iter == iter_times - 1)
//				R2 = FS_M;	//控制最后一次迭代时的输出文件数，以便和evidence矩阵做Map-Side Join
			inputPathA = outputPath_M_Q2Q;
			outputPath = outputPath_S_Q2Q2;
			if (shortKey)
			{
				success = MatrixMultiplyWithShortKey.runJob(conf, inputPathA, inputPathB,
					outputPath, tempPath, strategy, MM_R1, MM_R2, num_queries, num_ads, num_queries, IB, KB, JB,
				threshold_s, decay_factor, true, false, true);
			}
			else
			{
				success = MatrixMultiply3.runJob(conf, inputPathA, inputPathB,
						outputPath, tempPath, strategy, MM_R1, MM_R2, num_queries, num_ads, num_queries, IB, KB, JB,
					threshold_s, decay_factor, true, false, true);
			}
			
			//for debug
			if (DEBUG)
			{
				double[][] q_score = readMatrix(num_queries, num_queries, outputPath);
				for (int i = 0; i < num_queries; i++)
				{
					for (int j = 0; j < num_queries; j++)
						System.out.print("Q[" + i + "][" + j + "]=" + q_score[i][j] + " ");
					System.out.println();
				}
			}
		}

		return success;
	}
	
	/**
	 * 计算Ad-Ad之间的相似性分数矩阵
	 * 计算公式：A2A(k+1) = C * transpose(Q2Q(k) * Q2A) * Q2A
	 * 其中，Q2Q(k)表示第k轮迭代时Query-Query之间的相似性分数矩阵
	 * Q2A是Query-Ad的转移矩阵（在（带权值的）连接矩阵的基础上，按列归一化）
	 * transpose(A)表示矩阵A的转置矩阵
	 * @return 是否执行成功
	 * @throws Exception
	 */
	public boolean computeA2A() throws Exception
	{
		System.out.print("*************************************************<computing A2A score. ");
		System.out.println("Iterate " + (curr_iter + 1) + " of " + iter_times + ">**********************************");
		
		boolean success;
		String inputPathA;
		String inputPathB;
		String outputPath;
		//第一轮迭代，求Q-Q的相似性
		if (0 == curr_iter)
		{
			inputPathA = inputPath_W_Q2A;
			inputPathB = null;
			//inputPathB = inputPath_W_Q2A;
			outputPath = outputPath_S_A2A;
			
			if (shortKey)
			{
				success = MatrixMultiplyWithShortKey.runJob(conf, inputPathA, inputPathB,
					outputPath,	tempPath, strategy, MM_R1, MM_R2, num_ads, num_queries, num_ads, IB, KB, JB,
					threshold_s, decay_factor, true, true, true);
			}
			else
			{
				success = MatrixMultiply3.runJob(conf, inputPathA, inputPathB,
						outputPath,	tempPath, strategy, MM_R1, MM_R2, num_ads, num_queries, num_ads, IB, KB, JB,
						threshold_s, decay_factor, true, true, true);
			}
			
			//for debug
			if (DEBUG)
			{
				double[][] ad_score = readMatrix(num_ads, num_ads, outputPath);			
				for (int i = 0; i < num_ads; i++)
				{
					for (int j = 0; j < num_ads; j++)
						System.out.print("A[" + i + "][" + j + "]=" + ad_score[i][j] + " ");
					System.out.println();
				}
			}
			return success;
		}
		else
		{
			inputPathA = outputPath_S_Q2Q;
			inputPathB = inputPath_W_Q2A;
			outputPath = outputPath_M_A2A;
			if (shortKey)
			{
				success = MatrixMultiplyWithShortKey.runJob(conf, inputPathA, inputPathB,
					outputPath,	tempPath, strategy, MM_R1, MM_R2, num_queries, num_queries, num_ads, IB, KB, JB,
					threshold_s, 1.0f, false, false, false);
			}
			else
			{
				success = MatrixMultiply3.runJob(conf, inputPathA, inputPathB,
						outputPath,	tempPath, strategy, MM_R1, MM_R2, num_queries, num_queries, num_ads, IB, KB, JB,
						threshold_s, 1.0f, false, false, false);
			}
			
			
			//for debug
//			if (debug)
//			{
//				double[][] ad_score = readMatrix(num_queries, num_ads, outputPath);
//				for (int i = 0; i < num_queries; i++)
//				{
//					for (int j = 0; j < num_ads; j++)
//						System.out.println("AT[" + i + "][" + j + "]=" + ad_score[i][j]);
//				}
//			}
			
			inputPathA = outputPath_M_A2A;
			outputPath = outputPath_S_A2A;
			if (shortKey)
			{
				success = MatrixMultiplyWithShortKey.runJob(conf, inputPathA, inputPathB,
					outputPath, tempPath, strategy, MM_R1, MM_R2, num_ads, num_queries, num_ads, IB, KB, JB,
					threshold_s, decay_factor, true, false, true);
			}
			else
			{
				success = MatrixMultiply3.runJob(conf, inputPathA, inputPathB,
						outputPath, tempPath, strategy, MM_R1, MM_R2, num_ads, num_queries, num_ads, IB, KB, JB,
						threshold_s, decay_factor, true, false, true);
			}
			
			//for debug
			if (DEBUG)
			{
				double[][]ad_score = readMatrix(num_ads, num_ads, outputPath);
				for (int i = 0; i < num_ads; i++)
				{
					for (int j = 0; j < num_ads; j++)
						System.out.print("A[" + i + "][" + j + "]=" + ad_score[i][j] + " ");
					System.out.println();
				}
			}
		}

		return success;
	}
	
	/**	Prints a usage error message. */	
	int printUsage()
	{
		System.out.println("Usage: SimRankDriver args\nargs:"
				+ "\t\t-inputPath_QAS <input file qas>\n"
				+ "\t\t-inputPath_AQS <input file aqs>\n"
				+ "\t\t-workDirPath <work dir path>\n"
				+ "\t\t-inputPath_qQ <input file qQ(query code)>\n"
				+ "\t\t-inputPath_NQ <normlize-raw query dir path>\n"
				+ "\t\t[-mm_r1 <Reducer Num of matrix multiply job1>]\n"
				+ "\t\t[-mm_r2 <Reducer Num of matrix multiply job2>]\n"
				+ "\t\t[-wm_r <Reducer Num of weight matrix computation job, default: 1200>]\n"
				+ "\t\t[-r <Reducer Num of final origin score job, default: 200>]\n"
				+ "\t\t[-mm_max_split_size  <max split size for matrix multiply job, default: 48K>]\n"
				+ "\t\t[-mm_io_sort_mb  <io.sort.mb attribute for matrix multiply job, default: 1024>]\n"
				+ "\t\t[-fs_r  <reduce num of final score computation job, default: 600>]\n"
				+ "\t\t[-inplace <whether to use inplace version iteration algo, default: true>]\n"
				+ "\t\t[-strategy <1-4, the strategy of matrix multiply, default: 3>]\n"
				+ "\t\t[-block_size <the block size of sub matrixes, default: 20000>]\n"
				+ "\t\t[-iter_times <the iterate times of simrank algo, default: 5>]\n"
				+ "\t\t[-threshold_c <the threshold of click number for weight maxtix computing, default: 0>]\n"
				+ "\t\t[-threshold_s <the threshold of simrank score, default: 0.001>]\n"
				+ "\t\t[-defEvidenceValue <default evidence value, default: 0.5>]\n"
				+ "\t\t[-decay_factor <0-1, the decay factor of simrank algorithm, default: 0.6>]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	void printArgs(int block_size, int WM_R)
	{
		System.out.println("Weight Matrix Builder:");
		System.out.println("\tthreshold: " + this.threshold_c);
		System.out.println("\treduce: " + WM_R);
		System.out.println("SimRank:");
		System.out.println("\tdecay factor: " + this.decay_factor);
		System.out.println("\tthreshold: " + this.threshold_s);
		System.out.println("Matrix Multiply:");
		System.out.println("\tblock size: " + block_size);
		System.out.println("\tstrategy: " + this.strategy);
		System.out.println("\treduce num of job1: " + MM_R1);
		System.out.println("\treduce num of job2: " + MM_R2);
		System.out.println("Other:");
	}
	
	public void getQueryAndAdID(String path) throws IOException
	{
		//FileSystem fs = FileSystem.get(URI.create(path), conf);
		FileStatus[] files = fs.listStatus(new Path(path + "/query"));
		for (FileStatus file : files)
		{
			int id = Integer.parseInt(file.getPath().getName());
			if (num_queries < id)
			{
				num_queries = id;
			}
		}
		
		files = fs.listStatus(new Path(path + "/ad"));
		for (FileStatus file : files)
		{
			int id = Integer.parseInt(file.getPath().getName());
			if (num_ads < id)
			{
				num_ads = id;
			}
		}
		//删除临时目录
		//fs.delete(new Path(path), true);
	}
	@Override
	public int run(String[] args) throws Exception
	{
		int block_size = 20000;	//矩阵乘法的分块大小
		int FS_R = 400;	//计算final score的job所用的Reduce数,必须等于计算归一化Query所有的Reduce数，即GetWholeQueryText任务的Reduce数
		int WM_R = 1200; //计算权值矩阵时所用的Reduce数，决定了矩阵相乘时map的数量
		//int QT_R = 600;	//计算归一化Query所有的Reduce数，即GetWholeQueryText任务的Reduce数
		String inputPath_qQ = null;
		String inputPath_NQ = null;
		int R = 400;
		for (int i = 0; i < args.length; ++i)
		{
			if ("-inputPath_QAS".equals(args[i]))
				inputPath_QAS = args[++i];
			else if ("-inputPath_AQS".equals(args[i]))
				inputPath_AQS = args[++i];
			else if ("-inputPath_qQ".equals(args[i]))
				inputPath_qQ = args[++i];
			else if ("-inputPath_NQ".equals(args[i]))
				inputPath_NQ = args[++i];
			else if ("-workDirPath".equals(args[i]))
				inputPath_WorkDir = args[++i];
			else if ("-r".equals(args[i]))
				R = Integer.parseInt(args[++i]);
			else if ("-wm_r".equals(args[i]))
				WM_R = Integer.parseInt(args[++i]);			
			else if ("-mm_r1".equals(args[i]))
				MM_R1 = Integer.parseInt(args[++i]);
			else if ("-mm_r2".equals(args[i]))
				MM_R2 = Integer.parseInt(args[++i]);
			else if ("-fs_r".equals(args[i]))
				FS_R = Integer.parseInt(args[++i]);
//			else if ("-qt_r".equals(args[i]))
//				QT_R = Integer.parseInt(args[++i]);			
			else if ("-block_size".equals(args[i]))
				block_size = Integer.parseInt(args[++i]);
			else if ("-threshold_s".equals(args[i]))
				threshold_s = Double.parseDouble(args[++i]);
			else if ("-threshold_c".equals(args[i]))
				threshold_c = Integer.parseInt(args[++i]);
			else if ("-mm_max_split_size".equals(args[i]))
			{
				long mm_maxSplitSize = Long.parseLong(args[++i]);
				MatrixMultiplyWithShortKey.setMaxSplitSize(mm_maxSplitSize);
				MatrixMultiply3.setMaxSplitSize(mm_maxSplitSize);
				System.out.println("\tmm_max_split_size: " + mm_maxSplitSize);
			}
			else if ("-mm_io_sort_mb".equals(args[i]))
			{
				int mb = Integer.parseInt(args[++i]);
				MatrixMultiplyWithShortKey.setIOSortMb(mb);
				MatrixMultiply3.setIOSortMb(mb);
				System.out.println("\tmm_io_sort_mb: " + mb);
			}
			else if ("-decay_factor".equals(args[i]))
			{
				decay_factor = Float.parseFloat(args[++i]);
				if (decay_factor <= 0 || decay_factor > 1)
				{
					System.out.println("decay_factor should be between 0 and 1.");
					System.exit(-1);
				}
				System.out.println("\tdecay_factor: " + decay_factor);
			}
			else if ("-iter_times".equals(args[i]))
			{
				iter_times = Integer.parseInt(args[++i]);
				if (iter_times < 0)
				{
					System.out.println("iterate times should larger than 0.");
					System.exit(-1);
				}
				System.out.println("\titer_times: " + iter_times);
			}
			else if ("-strategy".equals(args[i]))
			{
				strategy = Integer.parseInt(args[++i]);
				if (strategy < 1 || strategy > 4)
				{
					System.err.println("strategy " + strategy + " is out of bound.");
					System.exit(-1);
				}
				System.out.println("\tstrategy: " + strategy);
			}
			else if ("-defEvidenceValue".equals(args[i]))
			{
				double defVal = Double.parseDouble(args[++i]);
				if (defVal > 0.5 || defVal <= 0)
				{
					System.err.println("defEvidenceValue is out of rang: (0, 0.5]");
					System.exit(-1);
				}
				//GetFinalScore.setDefaultEvidenceValue(defVal);
				JoinScoreAndEvidence.setDefaultEvidenceValue(defVal);
				System.out.println("\tdefEvidenceValue: " + defVal);
			}
			else if ("-inplace".equals(args[i]))
			{
				inplace = Boolean.parseBoolean(args[++i]);
				System.out.println("\tinplace: " + inplace);
			}
		}
		
		if (inputPath_QAS == null || inputPath_AQS == null || inputPath_WorkDir == null
				|| inputPath_QAS.isEmpty() || inputPath_AQS.isEmpty() || inputPath_WorkDir.isEmpty()	
				|| null == inputPath_qQ || null == inputPath_NQ )
		{
			printUsage();
			return -1;
		}
		
		printArgs(block_size, WM_R);
		
		//设置中间文件的输出和输入目录
		outputPath_S_A2A = inputPath_WorkDir + "/S_A2A";
		outputPath_S_Q2Q = inputPath_WorkDir + "/S_Q2Q";
		outputPath_S_Q2Q2 = inputPath_WorkDir + "/S_Q2Q2";
		outputPath_M_Q2Q = inputPath_WorkDir + "/M_Q2Q";
		outputPath_M_A2A = inputPath_WorkDir + "/M_A2A";
		tempPath = inputPath_WorkDir + "/TempMatrixMultiply";
		String OUTPUT_DIR_W_PATH = inputPath_WorkDir + "/weight_m";
		inputPath_W_A2Q = OUTPUT_DIR_W_PATH + "/WQA*";
		inputPath_W_Q2A = OUTPUT_DIR_W_PATH + "/WAQ*";
//		String DATA_DIR_PATH = "/group/tbalgo-dev/zhenghon/simrank";
//		inputPath_W_Q2A = DATA_DIR_PATH + "/Q2A_W";
//		inputPath_W_A2Q = DATA_DIR_PATH + "/A2Q_W";
		
		String OUTPUT_DIR_EVIDENCE = inputPath_WorkDir + "/evidence";
		String OUTPUT_FINAL_SCORE = inputPath_WorkDir + "/FinalScore";
		String OUTPUT_QUERY_TEXT = inputPath_WorkDir + "/queryText";
		String OUTPUT_QUERY_SCORE_TEXT = inputPath_WorkDir + "/QueryScoreText";
		String OUTPUT_FINAL_SCORE_ORIGIN = inputPath_WorkDir + "/FinalScoreOrigin";
		String OUTPUT_MAX_ID_DIR = inputPath_WorkDir + "/maxId";
		String OUTPUT_AGGREGATED_SCORE = inputPath_WorkDir + "/AggregatedScore";
		
		///////////////////////////////////////////////////
		fs = FileSystem.get(URI.create(inputPath_WorkDir), conf);
		//1. 抽取权值矩阵
		//reduce的数量决定了矩阵相乘时map的数量
		WeightMatrixBuilder.setThreshold(threshold_c);
		if (!WeightMatrixBuilder.runJob(inputPath_QAS, inputPath_AQS, OUTPUT_DIR_W_PATH, OUTPUT_MAX_ID_DIR, WM_R))
		{
			System.err.println("failed while building weight matrix");
			return -1;
		}
		
		//获取最大Query编号和最大广告编号
		getQueryAndAdID(OUTPUT_MAX_ID_DIR);		
		
		//编号从0开始，数量为最大编号加1
		++num_queries;
		++num_ads;		
		
		//设置子矩阵分块大小
		if (num_queries > block_size)
			IB = block_size;
		else
			IB = (int) (num_queries / 2) + 1;
		JB = IB;
		if (num_ads > block_size)
			KB = block_size;
		else
			KB = (int) (num_ads / 2) + 1;
		
		int NIB = (num_queries - 1) / IB + 1;
		int NKB = (num_ads - 1) / KB + 1;
		shortKey = (NIB < Short.MAX_VALUE && NKB < Short.MAX_VALUE);
		
		/////////////////////////////////////////////////////////////////////////////	
		//2. 运行SimRank迭代
		run();
		
		//删除中间结果文件、释放云梯空间	
		//fs.delete(new Path(OUTPUT_MAX_ID_DIR), true); //删除临时目录
		fs.delete(new Path(tempPath), true);
		fs.delete(new Path(outputPath_M_Q2Q), true);
		fs.delete(new Path(outputPath_M_A2A), true);
//		fs.delete(new Path(outputPath_S_A2A), true);
//		fs.delete(new Path(inputPath_W_A2Q), true);
//		fs.delete(new Path(inputPath_W_Q2A), true);
		
		//3. 抽取证据矩阵
		if (!EvidenceMatrixBuilder3.runJob(inputPath_AQS, OUTPUT_DIR_EVIDENCE, FS_R))
			return -1;
		
		//4. 计算最终的相似性分数
//		if (!GetFinalScore.runJob(OUTPUT_DIR_EVIDENCE, outputPath_S_Q2Q, OUTPUT_FINAL_SCORE, inputPath_WorkDir, FS_R))
//			return -1;
		if (!JoinScoreAndEvidence.runJob(OUTPUT_DIR_EVIDENCE, outputPath_S_Q2Q, OUTPUT_FINAL_SCORE, inputPath_WorkDir, FS_R))
			return -1;
		
		//删除中间结果文件、释放云梯空间
		//fs.delete(new Path(OUTPUT_DIR_EVIDENCE), true);
		
		//5. 格式转换
		GetWholeQueryText.runJob(inputPath_qQ, inputPath_NQ, OUTPUT_QUERY_TEXT, FS_R);
		
		//6. 得到最终结果
		//SimRankViewer.run(OUTPUT_QUERY_TEXT, OUTPUT_FINAL_SCORE, OUTPUT_FINAL_SCORE_ORIGIN, inputPath_WorkDir, FS_R, R);
		JoinQueryAndText.runJob(OUTPUT_FINAL_SCORE, OUTPUT_QUERY_TEXT, OUTPUT_QUERY_SCORE_TEXT, R);
		AggregateScore.runJob(OUTPUT_QUERY_SCORE_TEXT, OUTPUT_AGGREGATED_SCORE, R);
		GetOriginFinalScore.runJob(OUTPUT_QUERY_TEXT, OUTPUT_AGGREGATED_SCORE, OUTPUT_FINAL_SCORE_ORIGIN, R);
		return 0;
	}

	public static void main (String[] args) throws Exception	
	{
		final boolean TEST = true;
		if (TEST)
		{
			String INPUT_PATH_QAS = "/group/tbalgo-dev/zhouxj/aqr/20110829/qas/qaspart-00000";
			String INPUT_PATH_AQS = "/group/tbalgo-dev/zhouxj/aqr/20110829/aqs/";
			String WORK_DIR = "/group/tbalgo-dev/zhenghon/simrank";
			String inputPath_qQ = "/group/tbalgo-dev/zhouxj/aqr/20110829/qas/qQpart-00000"; 
			String inputPathNQ = "/group/tbalgo-dev/zhouxj/aqr/20110829/NQ";
			
			String[] params = {
				"-inputPath_QAS", INPUT_PATH_QAS, "-inputPath_AQS", INPUT_PATH_AQS,
				"-inputPath_qQ", inputPath_qQ, "-inputPath_NQ", inputPathNQ,
				"-workDirPath", WORK_DIR, "-mm_r1", "1200", "-mm_r2", "600"
				//,"-block_size", "20000", "-threshold_c", "30"
			};
			ToolRunner.run(new Configuration(), new SimRankDriver(), params);
		}
		else
		{
		    int res = ToolRunner.run(new Configuration(), new SimRankDriver(), args);
		    System.exit(res);
		}
	}
}
