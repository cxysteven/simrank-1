package com.taobao.research.jobs.simrank;

//HOST TABLE -> INDEX TABLE(A,B) -> Page TABLE

import java.io.*;
import java.util.*;

/**
 * This class implements a memory Data Structure for storing graphs.</p>
 * <p>
 * 
 * A large amount of research has recently focused on the graph structure (or
 * link structure) of the World Wide Web, which has proven to be extremely
 * useful for improving the performance of search engines and other tools for
 * navigating the web. For example, the PageRank algorithm of Brin and Page,
 * used in the Google search engine, and the HITS algorithm of Kleinberg both
 * rank pages according to the number and importance of other pages that link to
 * them.
 * </p>
 * <p>
 * 
 * This class provides the methods needed to efficiently compute with graphs and
 * to experiment with such algorithms, using main memory for storage.
 * @deprecated
 * @author Yang Xudong
 */
public class WebGraph
{

	/**
	 * A Map storing relationships from numeric identifiers to URLs, useful for
	 * storing Web graphs
	 */
	private Map<Integer, String> IdentifierToURL;

	/**
	 * A Map storing relationships from URLs to numeric identifiers, useful for
	 * storing Web graphs
	 * 数据格式：Map<主机名(host), Map<文件名(name), 编号(id)> >
	 * 按主机名对URL聚类
	 */
	private Map<String, Map<String, Integer> > URLToIdentifier;

	/**
	 * A Map storing InLinks. For each identifier (the key), another Map is
	 * stored, containing for each in-link an associated "connection weight"
	 */
	private Map<Integer, Map<Integer, Double> > InLinks;

	/**
	 * A Map storing OutLinks. For each identifier (the key), another Map is
	 * stored, containing for each in-link an associated "connection weight"
	 */
	private Map<Integer, Map<Integer, Double> > OutLinks;

	/** The number of nodes in the graph */
	private int nodeCount;

	/**
	 * Constructor for WebGraph
	 * 
	 */
	public WebGraph()
	{
		IdentifierToURL = new HashMap<Integer, String>();
		URLToIdentifier = new HashMap<String, Map<String, Integer> >();
		InLinks = new HashMap<Integer, Map<Integer, Double> >();
		OutLinks = new HashMap<Integer, Map<Integer, Double> >();
		nodeCount = 0;
	}

	/**
	 * Constructor for WebGraph, reading data from a text file. Each line of the
	 * file contains an association in the form:
	 * 
	 * http://url1.com -> http://url2.com 1.0
	 * 
	 * Stating that "http://url1.com" contains an out-link to "http://url2.com",
	 * with an associated connection strength of 1.0
	 * 
	 * @param aux
	 *            The name of the file
	 * @throws IOException
	 *             An error occurred while reading the file
	 * @throws FileNotFoundException
	 *             An error occurred while reading the file
	 */
	public WebGraph(File file) throws IOException, FileNotFoundException
	{
		this();
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String line;
		while ((line = reader.readLine()) != null)
		{
			int index1 = line.indexOf("->");
			if (index1 == -1)
				addLink(line.trim());
			else
			{
				String url1 = line.substring(0, index1).trim();
				String url2 = line.substring(index1 + 2).trim();
				Double strength = new Double(1.0);
				index1 = url2.indexOf(" ");
				if (index1 != -1)
					try
					{
						strength = new Double(url2.substring(index1 + 1).trim());
						url2 = url2.substring(0, index1).trim();
					} catch (Exception e)
					{
					}
				addLink(url1, url2, strength);
			}
		}
	}

	/**
	 * Returns the identifier associated with a given URL
	 * 
	 * @param URL
	 *            The URL
	 * @return The identifier associated with the given URL
	 */
	public Integer URLToIdentifier(String URL)
	{
		String host;
		String name;
		int index = 0, index2 = 0;
		if (URL.startsWith("http://"))
			index = 7;
		else if (URL.startsWith("ftp://"))
			index = 6;
		index2 = URL.substring(index).indexOf("/");
		if (index2 != -1)
		{
			name = URL.substring(index + index2 + 1);
			host = URL.substring(0, index + index2);
		}
		else
		{
			host = URL;
			name = "";
		}
		Map<String, Integer> map = URLToIdentifier.get(host);
		if (map == null)
			return null;
		return map.get(name);
	}

	/**
	 * Returns the URL associated with a given identifier
	 * 
	 * @param id
	 *            The identifier
	 * @return The URL associated with the given identifier
	 */
	public String IdentifyerToURL(Integer id)
	{
		return IdentifierToURL.get(id);
	}

	/**
	 * Adds a node to the graph
	 * 
	 * @param link
	 *            The URL associated with the added node
	 */
	public void addLink(String link)
	{
		Integer id = URLToIdentifier(link);
		if (id == null)
		{
			//id = new Integer(++nodeCount);
			id = new Integer(nodeCount++);	//从0开始编号
			String host;
			String name;
			int index = 0, index2 = 0;
			if (link.startsWith("http://"))
				index = 7;
			else if (link.startsWith("ftp://"))
				index = 6;
			index2 = link.substring(index).indexOf("/");
			if (index2 != -1)
			{
				name = link.substring(index + index2 + 1);
				host = link.substring(0, index + index2);
			} else
			{
				host = link;
				name = "";
			}
			Map<String, Integer> map = URLToIdentifier.get(host);
			if (map == null)
				map = new HashMap<String, Integer>();
			map.put(name, id);
			URLToIdentifier.put(link, map);
			IdentifierToURL.put(id, link);
			InLinks.put(id, new HashMap<Integer, Double>());
			OutLinks.put(id, new HashMap<Integer, Double>());
		}
	}

	/**
	 * Adds an association between two given nodes in the graph. If the
	 * corresponding nodes do not exists, this method creates them. If the
	 * connection already exists, the strength value is updated.
	 * 
	 * @param fromLink
	 *            The URL for the source node in the graph
	 * @param fromLink
	 *            The URL for the target node in the graph
	 * @param fromLink
	 *            The strength to associate with the connection
	 * @return The strength associated with the connection
	 */
	public Double addLink(String fromLink, String toLink, Double weight)
	{
		addLink(fromLink);
		addLink(toLink);
		Integer id1 = URLToIdentifier(fromLink);
		Integer id2 = URLToIdentifier(toLink);
		return addLink(id1, id2, weight);
	}

	/**
	 * Adds an association between two given nodes in the graph. If the
	 * corresponding nodes do not exists, this method creates them. If the
	 * connection already exists, the strength value is updated.
	 * 
	 * @param fromLink
	 *            The identifier for the source node in the graph
	 * @param fromLink
	 *            The identifier for the target node in the graph
	 * @param fromLink
	 *            The strength to associate with the connection
	 * @return The strength associated with the connection
	 */
	private Double addLink(Integer fromLink, Integer toLink, Double weight)
	{
		Double aux;
		Map<Integer, Double> map1 = InLinks.get(toLink);
		Map<Integer, Double> map2 = OutLinks.get(fromLink);
		aux = map1.get(fromLink);
		if (aux == null)
			map1.put(fromLink, weight);
		else if (aux.doubleValue() < weight.doubleValue())
			map1.put(fromLink, weight);
		else
			weight = new Double(aux.doubleValue());
		
		aux = map2.get(toLink);
		if (aux == null)
			map2.put(toLink, weight);
		else if (aux.doubleValue() < weight.doubleValue())
			map2.put(toLink, weight);
		else
		{
			weight = new Double(aux.doubleValue());
			map1.put(fromLink, weight);
		}
		InLinks.put(toLink, map1);
		OutLinks.put(fromLink, map2);
		return weight;

	}

	/**
	 * Returns a Map of the nodes that connect to a given node in the graph.
	 * Each mapping contains the identifier for a node and the associated
	 * connection strength.
	 * 
	 * @param URL
	 *            The URL for the node in the graph
	 * @return A Map of the nodes that connect to the given node in the graph.
	 */
	public Map<Integer, Double> inLinks(String URL)
	{
		Integer id = URLToIdentifier(URL);
		return inLinks(id);
	}

	/**
	 * Returns a Map of the nodes that connect to a given node in the graph.
	 * Each mapping contains the identifier for a node and the associated
	 * connection strength.
	 * 
	 * @param link
	 *            The identifier for the node in the graph
	 * @return A Map of the nodes that connect to the given node in the graph.
	 */
	public Map<Integer, Double> inLinks(Integer link)
	{
		if (link == null)
			return new HashMap<Integer, Double>();
		Map<Integer, Double> aux = InLinks.get(link);
		return (aux == null) ? new HashMap<Integer, Double>() : aux;
	}

	/**
	 * Returns a Map of the nodes that are connected from a given node in the
	 * graph. Each mapping contains the identifier for a node and the associated
	 * connection strength.
	 * 
	 * @param URL
	 *            The URL for the node in the graph
	 * @return A Map of the nodes that are connected from the given node in the
	 *         graph.
	 */
	public Map<Integer, Double> outLinks(String URL)
	{
		Integer id = URLToIdentifier(URL);
		return outLinks(id);
	}

	/**
	 * Returns a Map of the nodes that are connected from a given node in the
	 * graph. Each mapping contains the identifier for a node and the associated
	 * connection strength.
	 * 
	 * @param link
	 *            The URL for the node in the graph
	 * @return A Map of the nodes that are connected from the given node in the
	 *         graph.
	 */
	public Map<Integer, Double> outLinks(Integer link)
	{
		if (link == null)
			return new HashMap<Integer, Double>();
		Map<Integer, Double> aux = OutLinks.get(link);
		return (aux == null) ? new HashMap<Integer, Double>() : aux;
	}

	/**
	 * Returns the connection strength between two nodes, assuming there is a
	 * connection from the first to the second. If no connection exists, a link
	 * strength of zero is returned.
	 * 
	 * @param fromLink
	 *            The source link
	 * @param toLink
	 *            The target link
	 * @return The strenght for the connection between fromLink and toLink (
	 *         fromLink -> toLink )
	 * @see inLink
	 */
	public Double inLink(String fromLink, String toLink)
	{
		Integer id1 = URLToIdentifier(fromLink);
		Integer id2 = URLToIdentifier(toLink);
		return inLink(id1, id2);
	}

	/**
	 * Returns the connection strength between two nodes, assuming there is a
	 * connection from the first to the second. If no connection exists, a link
	 * strength of zero is returned.
	 * 
	 * @param fromLink
	 *            The source link
	 * @param toLink
	 *            The target link
	 * @return The strength for the connection between fromLink and toLink (
	 *         fromLink -> toLink )
	 * @see outLink
	 */
	public Double outLink(String fromLink, String toLink)
	{
		Integer id1 = URLToIdentifier(fromLink);
		Integer id2 = URLToIdentifier(toLink);
		return outLink(id1, id2);
	}

	/**
	 * Returns the connection strength between two nodes, assuming there is a
	 * connection from the first to the second. If no connection exists, a link
	 * strength of zero is returned.
	 * 
	 * @param fromLink
	 *            An identifier for the source link
	 * @param toLink
	 *            An identifier for the target link
	 * @return The strength for the connection between fromLink and toLink (
	 *         fromLink -> toLink )
	 * @see outLink
	 */
	public Double inLink(Integer fromLink, Integer toLink)
	{
		Map<Integer, Double> aux = inLinks(toLink);
		if (aux == null)
			return new Double(0);
		Double weight = (Double) (aux.get(fromLink));
		return (weight == null) ? new Double(0) : weight;
	}

	/**
	 * Returns the connection strength between two nodes, assuming there is a
	 * connection from the first to the second. If no connection exists, a link
	 * strength of zero is returned.
	 * 
	 * @param fromLink
	 *            An identifier for the source link
	 * @param toLink
	 *            An identifier for the target link
	 * @return The strength for the connection between fromLink and toLink (
	 *         fromLink -> toLink )
	 * @see inLink
	 */
	public Double outLink(Integer fromLink, Integer toLink)
	{
		Map<Integer, Double> aux = outLinks(fromLink);
		if (aux == null)
			return new Double(0);
		Double weight = (Double) (aux.get(toLink));
		return (weight == null) ? new Double(0) : weight;
	}

	/**
	 * Transforms a bi-directional graph to an uni-directional equivalent. The
	 * connection strength between two nodes A and B that are inter-connected in
	 * the bi-directional graph is transformed into
	 * MAX(weight_inlink(A,B),weight_outlink(A,B))
	 */
	public void transformUnidirectional()
	{
		Iterator<Integer> it = OutLinks.keySet().iterator();
		while (it.hasNext())
		{
			Integer link1 = it.next();
			Map<Integer, Double> auxMap = OutLinks.get(link1);
			Iterator<Integer> it2 = auxMap.keySet().iterator();
			while (it2.hasNext())
			{
				Integer link2 = it.next();
				Double weight = auxMap.get(link2);
				addLink(link2, link1, weight);
			}
		}
	}

	/**
	 * Remove nodes which correspond to an internal link. In a Web Graph,
	 * internal links are those made to pages that are situated on the same
	 * host.
	 */
	public void removeInternalLinks()
	{
		int index1;
		Iterator<Integer> it = OutLinks.keySet().iterator();
		while (it.hasNext())
		{
			Integer link1 = (Integer) (it.next());
			Map<Integer, Double> auxMap = OutLinks.get(link1);
			Iterator<Integer> it2 = auxMap.keySet().iterator();
			if (it2.hasNext())
			{
				String URL1 = (String) (IdentifierToURL.get(link1));
				index1 = URL1.indexOf("://");
				if (index1 != -1)
					URL1 = URL1.substring(index1 + 3);
				index1 = URL1.indexOf("/");
				if (index1 != -1)
					URL1 = URL1.substring(0, index1);
				while (it2.hasNext())
				{
					Integer link2 = (Integer) (it.next());
					String URL2 = (String) (IdentifierToURL.get(link2));
					index1 = URL2.indexOf("://");
					if (index1 != -1)
						URL2 = URL1.substring(index1 + 3);
					index1 = URL2.indexOf("/");
					if (index1 != -1)
						URL2 = URL1.substring(0, index1);
					if (URL1.equals(URL2))
					{
						auxMap.remove(link2);
						OutLinks.put(link1, auxMap);
						auxMap = InLinks.get(link2);
						auxMap.remove(link1);
						InLinks.put(link2, auxMap);
					}
				}
			}
		}
	}

	/**
	 * Remove nodes which correspond to nepotistic links. In a Web Graph,
	 * nepotistic links are typically those made to pages that are situated on
	 * the same host, corresponding to links made for hypertext navigational
	 * purposes rather than semantic similarity.
	 * 
	 * See the paper
	 * "<a href="http://www.cse.lehigh.edu/~brian/pubs/2000/aaaiws/
	 * ">Recognizing Nepotistic Links on the Web</a>" by Brian Davison,
	 * presented at the AAAI-2000 Workshop on Artificial Intelligence for Web
	 * Search, Austin, TX, July 30, and published in Artificial Intelligence for
	 * Web Search, Technical Report WS-00-01, pp. 23-28, AAAI Press.
	 * 
	 */
	public void removeNepotistic()
	{
		removeInternalLinks();
	}

	/**
	 * Remove nodes which correspond to stop URLs.
	 * 
	 * @param stopURLs
	 *            An array of Strings with the Stop URLs
	 */
	public void removeStopLinks(String stopURLs[])
	{
		Set<String> aux = new HashSet<String>();
		for (int i = 0; i < stopURLs.length; i++)
			aux.add(stopURLs[i]);
		removeStopLinks(aux);
	}

	/**
	 * Remove nodes which correspond to stop URLs. In a Web Graph, stop URLs
	 * correspond to very frequent pages. A link from/to such an URLs usually
	 * does not imply semantic similarity.
	 * 
	 * @param stopURLs
	 *            A Map where keys are the Stop URLs
	 */
	public void removeStopLinks(Set<String> stopURLs)
	{
		int index1;
		Iterator<Integer> it = OutLinks.keySet().iterator();
		while (it.hasNext())
		{
			Integer link1 = it.next();
			String URL1 = IdentifierToURL.get(link1);
			////////////////////////////////////////////
			//取URL的host
			index1 = URL1.indexOf("://");
			if (index1 != -1)
				URL1 = URL1.substring(index1 + 3);
			index1 = URL1.indexOf("/");
			if (index1 != -1)
				URL1 = URL1.substring(0, index1);
			/////////////////////////////////////////////
			if (stopURLs.contains(URL1))
			{
				OutLinks.put(link1, new HashMap<Integer, Double>());
				InLinks.put(link1, new HashMap<Integer, Double>());
			}
		}
	}

	/**
	 * Returns the number of nodes in the graph
	 * 
	 * @return The number of nodes in the graph
	 */
	public int numNodes()
	{
		return nodeCount;
	}

}