package com.taobao.research.jobs.simrank;

import java.util.*;

/**
 * SimRank is an iterative PageRank-like method for computing similarity. It
 * goes beyond direct cocitation for computing similarity much as PageRank goes
 * beyond direct linking for computing importance.
 * @deprecated
 * @author Yang Xudong
 */
public class SimRank
{

	/** The value for the SimRank dampening factor */
	private double dampening = 0.8;

	/** The data structure containing the Web linkage graph */
	private WebGraph graph;

	/** A <code>Map</code> containing the SimRank values for each page */
	//存储相似性分数的对称矩阵的下三角阵
	private Map<Integer, Map<Integer, Double> > scores;

	private int iter = 5;
	/**
	 * Constructor for SimRank
	 * 
	 * @param graph
	 *            The data structure containing the Web linkage graph
	 */
	public SimRank(WebGraph graph)
	{
		this.graph = graph;
		this.scores = new HashMap<Integer, Map<Integer, Double> >();
		int numLinks = graph.numNodes();
//		Double faux = new Double(1 / graph.numNodes());
		//所有值初始化为-1，表示还没用开始计算相似性分数
		for (int i = 0; i < numLinks; i++)
		{
			HashMap<Integer, Double> aux = new HashMap<Integer, Double>();
			for (int j = 0; j < i; j++)
				aux.put(new Integer(j), new Double(-1));
			scores.put(new Integer(i), aux);
		}
	}

	/**
	 * Sets the value for the SimRank dampening factor. The amount of SimRank
	 * that is transferred depends on a dampening factor which stands for ��the
	 * probability that a random surfer will get bored��. The dampening factor
	 * generally is set to 0.85.
	 * 
	 * @param damp
	 *            The dampening factor
	 */
	public void setDampening(double damp)
	{
		this.dampening = damp;
	}

	/**
	 * Returns the dampening factor used for the SimRank Algorithm. The amount
	 * of SimRank that is transferred depends on a dampening factor which stands
	 * for the probability that a random surfer will get bored��. The dampening
	 * factor generally is set to 0.85.
	 * 
	 * @return The dampening factor
	 */
	public double getDampening()
	{
		return this.dampening;
	}

	/**
	 * Computes the SimRank value for all the nodes in the Web Graph. In this
	 * method, the number of iterations of the algorithm is set accordingly to
	 * the number of nodes in the Web graph.
	 * 
	 */
	public void computeSimRank()
	{
		//int n = graph.numNodes();
		//int iter = ((int) Math.abs(Math.log((double) n)	/ Math.log((double) 10))) + 1;
		computeSimRank(iter);
	}

	/**
	 * Computes the SimRank value for all the nodes in the Web Graph. The
	 * procedure can be found on the article <a
	 * href="http://www-cs-students.stanford.edu/~glenj/simrank.pdf">SimRank: A
	 * Measure of Structural-Context Similarity</a>.
	 * 
	 * @param iter
	 *            The number of iterations for the algorithm
	 * 
	 */
	public void computeSimRank(int iter)
	{
		Double score = null;
		int numLinks = graph.numNodes();
		//相似性分数初始化为0，下三角矩阵
		for (int i = 0; i < numLinks; i++)
		{
			HashMap<Integer, Double> aux = new HashMap<Integer, Double>();
			for (int j = 0; j < i; j++)
				aux.put(new Integer(j), new Double(0));
			scores.put(new Integer(i), aux);
		}
		while ((iter--) > 0)
		{
			//用于存储新一轮迭代时计算出的相似性分数矩阵
			Map<Integer, Map<Integer, Double> > newScores = new HashMap<Integer, Map<Integer, Double> >();
			for (int id1 = 0; id1 < numLinks; id1++)
			{
				//map3是节点id1与其他节点的相似性分数向量，下三角矩阵的一行
				Map<Integer, Double> map3 = scores.get(new Integer(id1));
				//map1是连接到节点id1的所有节点及其连接权值
				Map<Integer, Double> map1 = graph.inLinks(new Integer(id1));
				//计算节点id1与其他节点的相似性分数
				for (int id2 = 0; id2 < id1; id2++)
				{
					////map2是连接到节点id2的所有节点及其连接权值
					Map<Integer, Double> map2 = graph.inLinks(new Integer(id2));
					int numInLinks1 = 0;	//|I(id1)|
					int numInLinks2 = 0;	//|I(id2)|
					score = new Double(0);
					boolean first = true;
					Iterator<Integer> it1 = map1.keySet().iterator();
					while (it1.hasNext())
					{
						Iterator<Integer> it2 = map2.keySet().iterator();
						Integer link1 = it1.next(); //one of the in-neighbors of id1 (E(id1))
						Double weight1 = map1.get(link1);//the weight of link1 to id1
						if (weight1 != null && weight1.doubleValue() > 0)
							numInLinks1++;
						while (it2.hasNext())
						{
							Integer link2 = it2.next();//one of the in-neighbors of id2 (E(id2))
							Double weight2 = map2.get(link2);//the weight of link1 to id2
							if (weight2 != null && weight2.doubleValue() > 0)
							{
								if (weight1 != null && weight1.doubleValue() > 0)
									score = new Double(simRank(link1, link2).doubleValue()
											+ score.doubleValue());
								if (first)
									numInLinks2++;
							}
						}
						first = false;
					}
						
					score = new Double((dampening / (numInLinks1 + numInLinks2)) * score.doubleValue());
					map3.put(new Integer(id2), score);
				}
				newScores.put(new Integer(id1), map3);
			}
			//更新相似性分数矩阵
			for (int j = 0; j < numLinks; j++)
			{
				scores.put(new Integer(j), newScores.get(new Integer(j)));
			}
		}
	}

	/**
	 * Returns the SimRank score between a given link and all other links in the
	 * Web graph
	 * 
	 * @param link
	 *            The url for the link
	 * @return A Map with the Amsler score between the given link and all other
	 *         links in the Web graph. Keys in the Map are link identifiers for
	 *         all the other links, and values correspond to the Amsler score
	 */
	public Map<Integer, Double> simRank(String link)
	{
		return simRank(graph.URLToIdentifier(link));
	}

	/**
	 * Returns the SimRank score between two given links
	 * 
	 * @param link1
	 *            The url for one of the links
	 * @param link2
	 *            The url for the other link
	 * @return The Amsler score between the two given links
	 */
	public Double simRank(String link1, String link2)
	{
		return simRank(graph.URLToIdentifier(link1),
				graph.URLToIdentifier(link2));
	}

	/**
	 * Returns the SimRank score between a given link identifier and all other
	 * links in the Web graph identifiers are Integer numbers, used in
	 * <code>WebGraph</code> to represent the Web graph for efficiency reasons.
	 * 
	 * @param link
	 *            The identifier for the link
	 * @return A Map with the Amsler score between the given link and all other
	 *         links in the Web graph. Keys in the Map are link identifiers for
	 *         all the other links, and values correspond to the Amsler score
	 */
	private Map<Integer, Double> simRank(Integer id)
	{
		if (id.intValue() != 0)
		{
			if (scores.get(id).get(new Integer(0)).doubleValue() < 0)
			{
				computeSimRank();
				return simRank(id);
			}
		}
		else
		{
			if (scores.get(new Integer(1)).get(new Integer(0)).doubleValue() < 0.0)
			{
				computeSimRank();
				return simRank(id);
			}
		}
		return scores.get(id);
	}

	/**
	 * Returns the SimRank score between two given link identifiers identifiers
	 * are Integer numbers, used in <code>WebGraph</code> to represent the Web
	 * graph for efficiency reasons.
	 * 
	 * @param link1
	 *            The identifier for one of the links
	 * @param link2
	 *            The identifier for the other link
	 * @return The Amsler score between the two given link identifiers
	 * @see WebGraph.identifierToURL()
	 */
	public Double simRank(Integer id1, Integer id2)
	{
		if (id1.equals(id2))
			return new Double(1);
		if (id2.intValue() > id1.intValue())
		{
			Integer id3 = id1;
			id1 = id2;
			id2 = id3;
		}
		Double aux = scores.get(id1).get(id2);
		if (aux.intValue() < 0)
		{
			computeSimRank();
			return scores.get(id1).get(id2);
		}
		return aux;
	}

	public int getIter()
	{
		return iter;
	}

	public void setIter(int iter)
	{
		this.iter = iter;
	}	
}