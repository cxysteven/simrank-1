package test.com.taobao.research.jobs.simrank;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.simrank.SimRank;
import com.taobao.research.jobs.simrank.SimRankDriver;
import com.taobao.research.jobs.simrank.WebGraph;

public class TestSimRank
{
	private static final String DATA_DIR_PATH = "/group/tbalgo-dev/zhenghon/simrank";
	private static final String INPUT_PATH_W_Q2A = DATA_DIR_PATH + "/Q2A_W";
	private static final String INPUT_PATH_W_A2Q = DATA_DIR_PATH + "/A2Q_W";
//	private static final String OUTPUT_PATH_S_Q2Q = DATA_DIR_PATH + "/Q2Q_S";
//	private static final String OUTPUT_PATH_M_Q2Q = DATA_DIR_PATH + "/Q2Q_M";
//	private static final String OUTPUT_PATH_S_A2A = DATA_DIR_PATH + "/A2A_S";
//	private static final String OUTPUT_PATH_M_A2A = DATA_DIR_PATH + "/A2A_M";
//	private static final String TEMP_DIR_PATH = DATA_DIR_PATH;
//	private static final String OUTPUT_PATH_S_Q2Q2 = DATA_DIR_PATH + "/Q2Q_S2";
	
	private static final String INPUT_GRAPH_PATH = "graph6_7.txt";

	private static Configuration conf = new Configuration();
	private static FileSystem fs;

	private static double[][] Q2A;
	private static double[][] A2Q;

	public static void writeMatrix(double[][] matrix, int rowDim, int colDim,
			String pathStr) throws IOException
	{
		Path path = new Path(pathStr);
		fs.delete(path, true);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
				IndexPair.class, DoubleWritable.class,
				SequenceFile.CompressionType.NONE);
//		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path,
//				IndexPair.class, FloatWritable.class,
//				SequenceFile.CompressionType.NONE);
		IndexPair indexPair = new IndexPair();
		DoubleWritable el = new DoubleWritable();
		//FloatWritable el = new FloatWritable();
		for (int i = 0; i < rowDim; i++)
		{
			for (int j = 0; j < colDim; j++)
			{
				double v = matrix[i][j];
				if (v != 0)
				{
					indexPair.index1 = i;
					indexPair.index2 = j;
					el.set(v);
					writer.append(indexPair, el);
				}
			}
		}
		writer.close();
	}

	private static void fillMatrix(double[][] matrix, Path path)
			throws IOException
	{
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
		IndexPair indexPair = new IndexPair();
		DoubleWritable el = new DoubleWritable();
		while (reader.next(indexPair, el))
		{
			matrix[indexPair.index1][indexPair.index2] = el.get();
		}
		reader.close();
	}

	public static double[][] readMatrix(int rowDim, int colDim, String pathStr)
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

	private static void testThreeByTwo () throws Exception		
	{
		Q2A = new double[][] { {1d/3d, 1d/3d}, {1d/3d, 1d/3d}, {1d/3d, 1d/3d}};
		A2Q = new double[][] { {1d/2d, 1d/2d, 1d/2d}, {1d/2d, 1d/2d, 1d/2d} };
		writeMatrix(Q2A, 3, 2, INPUT_PATH_W_Q2A);
		writeMatrix(A2Q, 2, 3, INPUT_PATH_W_A2Q);	
	}
	
	private static void testSixBySeven() throws Exception
	{
		Q2A = new double[][] {  {1d/3d, 1d/3d, 0d, 0d, 0d, 0d, 0d},
							    {1d/3d, 1d/3d, 0d, 0d, 0d, 0d, 0d},
							    {1d/3d, 1d/3d, 1d, 0d, 0d, 0d, 0d},
							    {0d, 0d, 0d, 1d, 1d, 0d, 0d},
							    {0d, 0d, 0d, 0d, 0d, 1d/2d, 1d/2d},
							    {0d, 0d, 0d, 0d, 0d, 1d/2d, 1d/2d}};
		A2Q = new double[][] {  {1d/2d, 1d/2d, 1d/3d, 0d, 0d, 0d},
								{1d/2d, 1d/2d, 1d/3d, 0d, 0d, 0d},
								{0d, 0d, 1d/3d, 0d, 0d, 0d},
								{0d, 0d, 0d, 1d/2d, 0d, 0d},
								{0d, 0d, 0d, 1d/2d, 0d, 0d},
								{0d, 0d, 0d, 0, 1d/2d, 1d/2d},
								{0d, 0d, 0d, 0, 1d/2d, 1d/2d}
								};
		writeMatrix(Q2A, 6, 7, INPUT_PATH_W_Q2A);
		writeMatrix(A2Q, 7, 6, INPUT_PATH_W_A2Q);
	}
	
	private static void testTwoByTwo() throws Exception
	{
		Q2A = new double[][] { {1d/2d, 1d/2d}, {1d/2d, 1d/2d}};
		A2Q = new double[][] { {1d/2d, 1d/2d}, {1d/2d, 1d/2d}};
		writeMatrix(Q2A, 2, 2, INPUT_PATH_W_Q2A);
		writeMatrix(A2Q, 2, 2, INPUT_PATH_W_A2Q);	
	}
	
	public static boolean withinDiff(double lh, double rh, double diff)
	{
		if (Math.abs(lh - rh) <= diff)
			return true;
		return false;
	}
	
	public static void main(String[] args) throws Exception
	{		
		//new GenericOptionsParser(conf, args);
		fs = FileSystem.get(conf);
		fs.mkdirs(new Path(DATA_DIR_PATH));

		String[] inputArgs = new String[8];
		//inputArgs[0] = OUTPUT_PATH_S_Q2Q2;
		inputArgs[1] = INPUT_PATH_W_A2Q;
		inputArgs[2] = INPUT_PATH_W_Q2A;
//		inputArgs[3] = OUTPUT_PATH_S_Q2Q;
//		inputArgs[4] = OUTPUT_PATH_M_Q2Q;
//		inputArgs[5] = OUTPUT_PATH_S_A2A;
//		inputArgs[6] = OUTPUT_PATH_M_A2A;
//		inputArgs[7] = TEMP_DIR_PATH;
		
		try
		{
			//testThreeByTwo();
			//testTwoByTwo();
			testSixBySeven();
			SimRankDriver.main(inputArgs);
		}
		finally
		{
//			fs.delete(new Path(OUTPUT_PATH_M_Q2Q), true);
//			fs.delete(new Path(OUTPUT_PATH_M_A2A), true);
		}

/*		WebGraph webGraph = new WebGraph(new File(INPUT_GRAPH_PATH));
		SimRank simRank = new SimRank(webGraph);
		simRank.computeSimRank();
		int n_q = 6;
		int n_a = 7;
		double[][] Q2Q = readMatrix(n_q, n_q, OUTPUT_PATH_S_Q2Q);
		double[][] A2A = readMatrix(n_a, n_a, OUTPUT_PATH_S_A2A);
		final double diff = 0.01;
		for (int i = 0; i < n_q; i++)
		{
			for (int j = 0; j < n_q; j++)
			{
				System.out.print("Q2Q[" + i + "][" + j + "]=" + Q2Q[i][j] + 
						", simRank(" + i + ", " + j + ")=" + simRank.simRank(i, j));
				if (!withinDiff(Q2Q[i][j], simRank.simRank(i, j), diff))
				{
					System.out.println(" diff");
					//return;
				}
				else
					System.out.println();
			}
		}
		for (int i = 0; i < n_a; i++)
		{
			for (int j = 0; j < n_a; j++)
			{
				System.out.print("A2A[" + i + "][" + j + "]=" + A2A[i][j] + 
						", simRank(" + i + ", " + j + ")=" + simRank.simRank(n_q+i, n_q+j));
				if (!withinDiff(A2A[i][j], simRank.simRank(n_q+i, n_q+j), diff))
				{
					System.out.println(" diff");
					//return;
				}
				else
					System.out.println();
			}
		}	
		System.out.println();
		System.out.println("================");
		System.out.println("All tests passed");
		System.out.println("================");
		System.out.println();
*/
	}

}
