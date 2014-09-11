package test.com.taobao.research.jobs.simrank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;

import com.taobao.research.jobs.common.IndexPair;
import com.taobao.research.jobs.matrix_multiply.MatrixMultiply;
import com.taobao.research.jobs.simrank.WeightMatrixBuilder;

public class TestWeightMatrixBuilder
{
	private static final String DATA_DIR_PATH = "/group/tbalgo-dev/zhenghon/simrank";
	private static final String INPUT_PATH_QAS = DATA_DIR_PATH + "/qas.txt";
	private static final String INPUT_PATH_AQS = DATA_DIR_PATH + "/aqs.txt";
	private static final String OUTPUT_DIR = DATA_DIR_PATH + "/W_OUTPUT";
	
	private static Configuration conf = new Configuration();
	private static FileSystem fs;
	
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
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception
	{
		fs = FileSystem.get(conf);
		WeightMatrixBuilder.runJob(INPUT_PATH_QAS, INPUT_PATH_AQS, OUTPUT_DIR, null, 200);
		
//		int n_q = 3;
//		int n_a = 2;
//		double[][] W_A2Q = readMatrix(n_q, n_a, OUTPUT_DIR);
	}

}
