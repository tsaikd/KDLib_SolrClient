package org.tsaikd.java.utils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

public class SolrUtils {

	static Log log = LogFactory.getLog(SolrUtils.class);

	public static String getUrl(String url) {
		if (url.startsWith("/")) {
			url = "http://127.0.0.1:8983" + url;
		} else if (url.startsWith(":")) {
				url = "http://127.0.0.1" + url;
		} else if (!url.startsWith("http") && !url.contains("/")) {
			url = "http://127.0.0.1:8983/solr/" + url;
		}
		return url;
	}

	public static <T> void sync(String urlSrc, String urlTar, Class<T> docType, int syncRow, String syncField) throws Exception {
		urlSrc = getUrl(urlSrc);
		urlTar = getUrl(urlTar);
		log.debug("Solr source server: " + urlSrc);
		SolrServer solrSrc = new HttpSolrServer(urlSrc);
		log.debug("Solr target server: " + urlTar);
		SolrServer solrTar = new HttpSolrServer(urlTar);

		SolrQuery query = new SolrQuery();
		QueryResponse res;
		List<T> list;
		T doc = null;
		String q;
		long rest;
		Date lastSync = null;
		ProcessEstimater pe = null;

		while (true) {
			if (lastSync == null) {
				query.setQuery("*:*");
				query.setSortField(syncField, SolrQuery.ORDER.desc);
				query.setRows(1);
				res = solrTar.query(query);
				list = res.getBeans(docType);
				if (!list.isEmpty()) {
					doc = list.get(0);
					lastSync = (Date) doc.getClass().getField(syncField).get(doc);
					lastSync = new Date(lastSync.getTime() + 1);
				}
			}

			if (lastSync == null) {
				q = syncField + ":[* TO *]";
			} else {
				DateFormat fmtInput = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				fmtInput.setTimeZone(TimeZone.getTimeZone("GMT"));
				q = syncField + ":[" + fmtInput.format(lastSync) + " TO *]";
			}
			query.setQuery(q);
			query.setSortField(syncField, SolrQuery.ORDER.asc);
			query.setRows(syncRow);
			res = solrSrc.query(query);
			rest = res.getResults().getNumFound();
			if (pe == null) {
				pe = new ProcessEstimater(rest);
			}
			pe.setRestNum(rest);
			log.debug("Rest: " + rest + ", Query:" + query.getQuery() + ", Time: " + pe.getRestString());
			list = res.getBeans(docType);
			if (list.isEmpty()) {
				break;
			}
			solrTar.addBeans(list);
			solrTar.commit();
			doc = list.get(list.size()-1);
			lastSync = (Date) doc.getClass().getField(syncField).get(doc);
			lastSync = new Date(lastSync.getTime() + 1);
		}
	}

	public static <T> void sync(String urlSrc, String urlTar, Class<T> docType, int syncRow) throws Exception {
		String syncField = "solrIndexTime";
		sync(urlSrc, urlTar, docType, syncRow, syncField);
	}

	public static HttpEntity queryToEntity(HttpServletRequest req, HttpServletResponse res, HttpSolrServer solrServer, SolrQuery query) throws ClientProtocolException, IOException {
		HttpClient client = solrServer.getHttpClient();
		String solrUrl = solrServer.getBaseURL() + "/select/";
		HttpPost method = new HttpPost(solrUrl);

		if (log.isDebugEnabled()) {
			log.debug(query.getQuery());
			if (req != null) {
				log.debug(ServletUtils.getRemoteRealIP(req) + ": " + solrUrl + "?" + query.toString());
			}
		}
		method.setEntity(new StringEntity(query.toString(), ContentType.create("application/x-www-form-urlencoded", "UTF-8")));
		HttpResponse cliRes = client.execute(method);

		int status = cliRes.getStatusLine().getStatusCode();
		if (status != HttpStatus.SC_OK) {
			if (res != null) {
				res.setStatus(status);
			}
		}

		return cliRes.getEntity();
	}

	public static String queryToString(HttpServletRequest req, HttpServletResponse res, HttpSolrServer solrServer, SolrQuery query) throws ClientProtocolException, IOException {
		HttpEntity entity = queryToEntity(req, res, solrServer, query);
		return EntityUtils.toString(entity, "UTF-8");
	}

	public static BasicDBObject queryToDBObject(HttpServletRequest req, HttpServletResponse res, HttpSolrServer solrServer, SolrQuery query) throws ClientProtocolException, IOException {
		String queryRes = queryToString(req, res, solrServer, query);
		return (BasicDBObject) JSON.parse(queryRes);
	}

	public static void queryToServletOutput(HttpServletRequest req, HttpServletResponse res, HttpSolrServer solrServer, SolrQuery query) throws ServletException, IOException {
		HttpEntity entity = queryToEntity(req, res, solrServer, query);
		ServletOutputStream out = res.getOutputStream();
		byte[] resData = EntityUtils.toByteArray(entity);
		out.write(resData);
		out.close();
	}

}
