/*
	<!-- TODO:LVZR -->
	<requestHandler name="/_cache" class="com.epwk.www.requesthandler.ReRankingCacheHandler">
	</requestHandler>

    <requestHandler name="/select-plus" class="com.epwk.www.requesthandler.ReRankingHandler">
        <lst name="defaults">
            <str name="echoParams">explicit</str>
            <int name="rows">10</int>
            <str name="df">text</str>
            <str name="defType">edismax</str>
            <str name="bf">
                recip(rord(views),1,20,20)^20 recip(rord(sale_num),1,30,30)^20 div(w_good_rate,5) spdj(9,-35,shop_level,6,7,8,9,10) w_level^1 div(credit_score,12) user_type^0.5
            </str>
            <!-- <str name="qf">title^8 indus_names div(shop_name,2)</str> -->
            <str name="mm">20%</str>
            <str name="q.alt">*:*</str>
            <str name="fl">*,score</str>
        </lst>
        <lst name="plus-init-args">
            <int name="defaultPageSize">40</int>
            <int name="limitPageNo">100</int>
            <str name="baseSolrUrls">http://10.0.102.71:8983/solr/service;http://10.0.102.72:8983/solr/service;http://10.0.102.73:8983/solr/service</str>
            <str name="defFq">is_close:0,is_stop:0,shop_id:{0 TO *}</str>
            <str name="wt">json-plus</str>
        </lst>
    </requestHandler>

    <queryResponseWriter name="json-plus" class="org.apache.solr.response.JSONResponseWriterPlus">
        <str name="content-type">application/json; charset=UTF-8</str>
    </queryResponseWriter>
 */
package com.lvzr.solr.requesthandler;

//import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/3/31.
 * <p>
 * use the following configurations in file solrconfig.xml
 * ...
 */
public class ReRankingHandler extends SearchHandler implements InitializingBean {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static final String SERVICE_ID = "service_id";

	private static final String SHOP_ID = "shop_id";

	private final String[] IDS = new String[]{SERVICE_ID, SHOP_ID};

//	private final ObjectMapper objectMapper = new ObjectMapper();

	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	private final Semaphore semaphore = new Semaphore(2);

	private final AtomicBoolean FAILED_WRITE_TO_CACHE = new AtomicBoolean(true);

	private final Map<String, AtomicInteger> _CACHE_SHOP_ID = new HashMap<>(1024);

	private List<List<String>> _CACHE_PAGE_TAB;

	private List<String> _CACHE_ALL_SERVICE_IDS;

	public static final String _API_SELECT_PLUS = "/select-plus";

	private static final String _API__CACHE = "/_cache";

	private volatile long numFound;

	private int defaultPageSize = 40;

	private int limitPageNo = 100;

	private String[] defFq = null;

	private final String[] wt = new String[1];

	private SolrClient solrClient;

	@Override
	public void init(PluginInfo info) {
		super.init(info);
		final Object plusInitArgs = super.initArgs.get("plus-init-args");
		if (null == plusInitArgs) {
			throw new IllegalArgumentException("plus-init-args参数配置为空！");
		}
		if (!(plusInitArgs instanceof NamedList)) {
			throw new RuntimeException("Init ReRankingHandler error.");
		}
		final SolrParams solrParams = ((NamedList<?>) plusInitArgs).toSolrParams();
		this.defaultPageSize = Optional.ofNullable(solrParams.getInt("defaultPageSize"))
				.orElse(this.defaultPageSize);
		this.limitPageNo = Optional.ofNullable(solrParams.getInt("limitPageNo"))
				.orElse(this.limitPageNo);
		this.defFq = Optional.ofNullable(solrParams.get("defFq")).orElse("").split(",");
		this.wt[0] = Optional.ofNullable(solrParams.get(CommonParams.WT)).orElse(CommonParams.JSON);
		final String baseSolrUrls = Optional.ofNullable(solrParams.get("baseSolrUrls"))
				.orElseThrow(() -> new IllegalArgumentException("baseSolrUrl could not be null."));
		this.solrClient = createLBHttpSolrClient(baseSolrUrls);
		this._CACHE_PAGE_TAB = new ArrayList<>(this.limitPageNo);
		this._CACHE_ALL_SERVICE_IDS = new ArrayList<>(this.limitPageNo * this.defaultPageSize);
		log.warn("plus-init args:{}", plusInitArgs.toString());
	}

	private LBHttpSolrClient createLBHttpSolrClient(final String baseSolrUrls) {
		return new LBHttpSolrClient.Builder()
				.withBaseSolrUrls(baseSolrUrls.split(";"))
				.build();
	}

	/**
	 * TODO: group=true&group.field=shop_id&group.limit=-1
	 *
	 * @param req ..
	 * @param rsp ..
	 * @throws Exception ..
	 */
	@Override
	public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		final String apiPath = req.getPath();
		switch (apiPath) {
			case _API_SELECT_PLUS:
				queryHandle(req, rsp);
				return;
			case _API__CACHE:
				cacheStat(req, rsp);
				return;
			default:
				rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + apiPath));
		}
	}

	private void cacheStat(SolrQueryRequest req, SolrQueryResponse rsp) {
		final String httpMethod = req.getHttpMethod();
		if (SolrRequest.METHOD.DELETE.equals(SolrRequest.METHOD.valueOf(httpMethod))) {
			resetCache();
		}
		Map<String, Object> map = new HashMap<>(4);
		map.put("_CACHE_SHOP_ID", _CACHE_SHOP_ID);
		map.put("_CACHE_PAGE_TAB", _CACHE_PAGE_TAB);
		map.put("_CACHE_ALL_SERVICE_IDS", _CACHE_ALL_SERVICE_IDS);
		map.put("all_data_size", _CACHE_ALL_SERVICE_IDS.size());
		map.put("cache_status", !FAILED_WRITE_TO_CACHE.get());
		rsp.addResponse(map);
	}

	private void queryHandle(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		final SolrParams params = req.getParams();
		if (!(params instanceof MultiMapSolrParams)) {
			super.handleRequestBody(req, rsp);
			return;
		}
		if (semaphore.tryAcquire(2)) {
			executor.submit(() -> {
				try {
					writeCache(super.defaults);
				} catch (Exception e) {
					log.error("write cache err", e);
					FAILED_WRITE_TO_CACHE.set(true);
				}
				semaphore.release();
			});
		}
		final Map<String, String[]> map = ((MultiMapSolrParams) params).getMap();
		final int originalRows = Optional.ofNullable(params.getInt(CommonParams.ROWS)).orElse(defaultPageSize);
		final int originalStart = Optional.ofNullable(params.getInt(CommonParams.START)).orElse(0);
		final int index = originalStart / originalRows;
		if (limitPageNo < index + 1) {
			super.handleRequestBody(req, rsp);
			return;
		}
		final int atLeastNeed = originalStart + originalRows;
		while (true) {
			if ((semaphore.availablePermits() == 1 && FAILED_WRITE_TO_CACHE.get()) || originalStart >= numFound) {
				if (numFound != 0) {
					super.handleRequestBody(req, rsp);
					return;
				}
			}
			final int pageTabSize = _CACHE_PAGE_TAB.size();
			final int nowTotalNum = _CACHE_ALL_SERVICE_IDS.size();
			if ((index >= pageTabSize || atLeastNeed > nowTotalNum) && numFound != nowTotalNum) {
				TimeUnit.MILLISECONDS.sleep(1);
				continue;
			}
			final List<String> serviceIds = new ArrayList<>(originalRows);
			if (defaultPageSize == originalRows && index < pageTabSize && originalStart % originalRows == 0) {
				final List<String> pageContent = _CACHE_PAGE_TAB.get(index);
				if (originalRows == pageContent.size()) {
					serviceIds.addAll(pageContent);
				}
			}
			if (serviceIds.isEmpty()) {
				if (failedSubListAndAddAllTo(serviceIds, originalStart, Math.min(atLeastNeed, nowTotalNum))) {
					continue;
				}
				if (serviceIds.size() != originalRows) {
					super.handleRequestBody(req, rsp);
					return;
				}
			} else if (originalRows != serviceIds.size()) {
				TimeUnit.MILLISECONDS.sleep(1);
				continue;
			}
			final String tmp = serviceIds.toString();
			final Map<String, String[]> paramsMap = new HashMap<>(map);
			paramsMap.remove(CommonParams.START);
			//  java.util.Map.replace(K, V)或java.util.Map.replace(K, V, V)方法，如果key不存在不做任何操作
			paramsMap.put(CommonParams.ROWS, new String[]{String.valueOf(serviceIds.size())});
			paramsMap.put(CommonParams.FQ, new String[]{String.format("%s:(%s)", SERVICE_ID,
					tmp.substring(1, tmp.length() - 1).replace(",", " "))});
			req.setParams(new MultiMapSolrParams(paramsMap));
			log.debug("[execute query by cache] QueryString:{}", req.getParams().toQueryString());
			super.handleRequestBody(req, rsp);
			final Map<String, String[]> originParamsMap = new HashMap<>(map);
			originParamsMap.put("num_found", new String[]{String.valueOf(numFound)});
			originParamsMap.put(CommonParams.WT, this.wt);
			req.setParams(new MultiMapSolrParams(originParamsMap));
			return;
		}
	}

	private boolean failedSubListAndAddAllTo(final List<String> serviceIds, final int fromIndex, final int toIndex) {
		final List<String> subList;
		try {
			subList = _CACHE_ALL_SERVICE_IDS.subList(fromIndex, toIndex);
		} catch (Exception e) {
			return true;
		}
		return !serviceIds.addAll(subList);
	}

	public void filter(Iterator<SolrDocument> solrDocumentIterator) throws Throwable {
		while (solrDocumentIterator.hasNext()) {
			final SolrDocument doc = solrDocumentIterator.next();
			final String shopId = String.valueOf(Optional.ofNullable(doc.get(SHOP_ID))
					.orElse(""));
			final String serviceId = String.valueOf(Optional.ofNullable(doc.get(SERVICE_ID))
					.orElse(""));
			final int pageTabSize = _CACHE_PAGE_TAB.size();
			int index = pageTabSize;
			if (_CACHE_SHOP_ID.containsKey(shopId)) {
				index = _CACHE_SHOP_ID.get(shopId).incrementAndGet();
			} else {
				for (int i = 0; i < pageTabSize; i++) {
					final int size = _CACHE_PAGE_TAB.get(i).size();
					if (size < defaultPageSize) {
						index = i;
						break;
					}
				}
				_CACHE_SHOP_ID.put(shopId, new AtomicInteger(index));
			}
			if (index >= pageTabSize) {
				final List<String> set = new ArrayList<>(defaultPageSize);
				set.add(serviceId);
				_CACHE_PAGE_TAB.add(set);
			} else {
				List<String> tmp;
				while ((tmp = _CACHE_PAGE_TAB.get(index)).size() >= defaultPageSize) {
					index = _CACHE_SHOP_ID.get(shopId).incrementAndGet();
					if (index >= pageTabSize) {
						tmp = new ArrayList<>(defaultPageSize);
						_CACHE_PAGE_TAB.add(tmp);
						break;
					}
				}
				tmp.add(serviceId);
			}
			solrDocumentIterator.remove();
			final int tmpSize;
			if ((tmpSize = _CACHE_PAGE_TAB.get(index).size()) > defaultPageSize) {
				final IllegalStateException illegalStateException = new IllegalStateException(
						String.format("PAGE_TAB's size greater than DEFAULT_PAGE_SIZE (index:%s, %s > %s)",
								index, tmpSize, defaultPageSize));
				_CACHE_PAGE_TAB.clear();
				_CACHE_SHOP_ID.clear();
				throw new Throwable(illegalStateException);
			}
		}
	}

	/**
	 * @param params {
	 *               "q": "*",
	 *               "fl": "service_id,pic,unite_price,app_pic,title,shop_id,price,unite_price_char,sale_num,uid,indus_ids,task_mark_num",
	 *               "start": "0",
	 *               "fq": [
	 *               "is_close:0",
	 *               "is_stop:0",
	 *               "shop_id:{0 TO *}"
	 *               ],
	 *               "rows": "40",
	 *               "wt": "json"
	 *               }
	 */
	private void writeCache(SolrParams params) throws IOException, SolrServerException {
		log.warn("Start to write cache.");
		final Map<String, String[]> map = new HashMap<>(((MultiMapSolrParams) Optional.ofNullable(params)
				.orElse(super.defaults)).getMap());
		if (null != this.defFq && 0 < this.defFq.length) {
			map.put(CommonParams.FQ, this.defFq);
		}
		final QueryResponse res = solrClient.query(new MultiMapSolrParams(map));
		final SolrDocumentList results = res.getResults();
		if (null == results) {
			throw new RuntimeException("null results;failed to set numFound.");
		}
		this.numFound = results.getNumFound();
		writeCache(map);
	}

	private void writeCache(final Map<String, String[]> map) throws IOException, SolrServerException {
		final AtomicInteger curPageNo = new AtomicInteger();
		final String[] rows = {String.valueOf(defaultPageSize)};
		map.put(CommonParams.FL, this.IDS);
		map.put(CommonParams.ROWS, rows);
		//  ...
		final StringBuilder params = new StringBuilder();
		map.forEach((k, v) -> {
			for (String s : v) {
				params.append(k).append('=').append(s).append('&');
			}
		});
		final String paramsPrefix = params.append(CommonParams.START).append('=').toString();
		//  ---
		final String[] start = new String[1];
		while (true) {
			final int pageNo = curPageNo.getAndIncrement();
			if (pageNo + 1 > limitPageNo) {
				log.warn("[pageNo>limitPageNo] -> execute [break]");
				break;
			}
			start[0] = String.valueOf((pageNo) * defaultPageSize);
			map.put(CommonParams.START, start);
			log.debug("QueryParams:{}", /*objectMapper.writeValueAsString(map)*/paramsPrefix + start[0]);
			final MultiMapSolrParams multiMapSolrParams = new MultiMapSolrParams(map);
			final QueryResponse query = solrClient.query(multiMapSolrParams);
			final SolrDocumentList results = query.getResults();
			final Iterator<SolrDocument> solrDocumentIterator;
			if (null == results || !(solrDocumentIterator = results.iterator()).hasNext()) {
				log.warn("[null == results OR !(solrDocumentIterator = results.iterator()).hasNext()]");
				break;
			}
			try {
				filter(solrDocumentIterator);
			} catch (Throwable throwable) {
				FAILED_WRITE_TO_CACHE.set(true);
				log.error("[filter execute failed] write cache err", throwable.getCause());
				return;
			}
		}
		_CACHE_PAGE_TAB.forEach(_CACHE_ALL_SERVICE_IDS::addAll);
		FAILED_WRITE_TO_CACHE.set(false);
	}

	private void resetCache() {
		_CACHE_SHOP_ID.clear();
		_CACHE_PAGE_TAB.clear();
		_CACHE_ALL_SERVICE_IDS.clear();
		semaphore.release();
	}

	@Override
	public String getDescription() {
		return super.getDescription();
	}

	@Override
	public void afterPropertiesSet() {
		log.warn("{}", Thread.currentThread().getStackTrace()[1].toString());
	}
}
