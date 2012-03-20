package org.tsaikd.java.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolrClient {

	static Log log = LogFactory.getLog(SolrClient.class);

	public static void testDependProject() throws ClassNotFoundException {
		// test KDJLib_SolrClient
		Class.forName("org.apache.solr.client.solrj.SolrQuery");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		log.debug("Start");
		log.info("End");
	}

}
