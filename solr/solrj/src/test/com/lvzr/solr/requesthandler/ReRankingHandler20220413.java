/*
	<!-- TODO:LVZR -->
    <requestHandler name="/select-plus" class="com.lvzr.solr.requesthandler.ReRankingHandler">
        <lst name="defaults">
            <str name="echoParams">explicit</str>
            <int name="rows">10</int>
            <str name="df">text</str>
            <!-- Change from JSON to XML format (the default prior to Solr 7.0)
                      <str name="wt">xml</str>
         -->
            <!-- Query settings -->
            <str name="defType">edismax</str>
            <str name="bf">
                <!-- recip(rord(views),1,15,15)^15 recip(rord(sale_num),1,40,40)^40 div(w_good_rate,15) map(map(linear(shop_level,3,-15),17,20,1.0),-15,1,1.0) w_level^0.4 div(credit_score,10) user_type^0.5 -->
                <!-- recip(rord(views),1,20,20)^20 recip(rord(sale_num),1,30,30)^20 div(w_good_rate,5) map(max(map(linear(shop_level,3,-5),17,20,1),0),0,0,0) w_level^1 div(credit_score,12) user_type^0.5 -->
                <!-- recip(rord(views),1,20,20)^20 recip(rord(sale_num),1,30,30)^20 div(w_good_rate,5) max(linear(shop_level,3,-5),1) w_level^1 div(credit_score,12) user_type^0.5 -->
                <!-- recip(rord(views),1,20,20)^20 recip(rord(sale_num),1,30,30)^20 div(w_good_rate,5) map(map(linear(shop_level,3,-5),17,20,1.0),-5,1,1.0) w_level^1 div(credit_score,12) user_type^0.5 -->
                recip(rord(views),1,20,20)^20 recip(rord(sale_num),1,30,30)^20 div(w_good_rate,5) spdj(9,-35,shop_level,6,7,8,9,10) w_level^1 div(credit_score,12) user_type^0.5
            </str>
            <!-- <str name="qf">title^8 indus_names div(shop_name,2)</str> -->
            <str name="mm">20%</str>
            <str name="q.alt">*:*</str>
            <str name="fl">*,score</str>
        </lst>
        <!-- <lst name="plus-init-args">
            <int name="defaultPageSize">40</int>
            <int name="limitPageNo">100</int>
            <str name="baseSolrUrl">http://localhost:8888/solr/test_service</str>
            <str name="wt">json-plus</str>
        </lst> -->
        <lst name="plus-init-args">
            <int name="defaultPageSize">5</int>
            <int name="limitPageNo">100</int>
            <str name="wt">json-plus</str>
            <str name="baseSolrUrl">http://localhost:8888/solr/test_service</str>
            <!-- optional -->
            <!-- <lst name="baseSolrUrls">
                <str>https://solr1.api.epweike.net/service</str>
                <str>https://solr2.api.epweike.net/service</str>
                <str>https://solr3.api.epweike.net/service</str>
            </lst> -->
        </lst>
    </requestHandler>

    <queryResponseWriter name="json-plus" class="org.apache.solr.response.JSONResponseWriterPlus">
        <str name="content-type">application/json; charset=UTF-8</str>
    </queryResponseWriter>
 */
package com.lvzr.solr.requesthandler;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.sun.istack.internal.Nullable;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
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
public class ReRankingHandler20220413 extends SearchHandler /*RequestHandlerBase implements PluginInfoInitialized*/ {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static final String SERVICE_ID = "service_id";

	private static final String SHOP_ID = "shop_id";

	private final String[] IDS = new String[]{SERVICE_ID, SHOP_ID};

	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	private final Semaphore semaphore = new Semaphore(2);

	private final AtomicBoolean FAILED_WRITE_TO_CACHE = new AtomicBoolean();

	private final Map<String, AtomicInteger> _CACHE_SHOP_ID = new HashMap<>(1024);

	private List<List<String>> _CACHE_PAGE_TAB;

	private List<String> _CACHE_ALL_SERVICE_IDS;

	public static final String _API_SELECT_PLUS = "/select-plus";

	private static final String _API__CACHE = "/_cache";

	private volatile long numFound;

	private int defaultPageSize = 40;

	private int limitPageNo = 100;

	private String wt = CommonParams.JSON;

	private SolrClient solrClient;

	private final Map<String, BiConsumerPlus<SolrQueryRequest, SolrQueryResponse>> API_CONSUMER_MAP = new HashMap<>(3);

	private final Runnable runnable = () -> {
		try {
			writeCache(null);
		} catch (IOException | SolrServerException e) {
			log.error("write cache err", e);
			//  标记失败
			FAILED_WRITE_TO_CACHE.set(true);
		}
		semaphore.release();
	};

	@Override
	public void init(PluginInfo info) {
		super.init(info);
//				super.init(info.initArgs);
		final Object plusInitArgs = super.initArgs.get("plus-init-args");
		if (null == plusInitArgs) {
			throw new IllegalArgumentException("plus-init-args参数配置为空！");
		}
		if (!(plusInitArgs instanceof NamedList)) {
			throw new RuntimeException("Init ReRankingHandler error.");
		}
		final SolrParams solrParams = ((NamedList<?>) plusInitArgs).toSolrParams();
		// TODO
            /*//noinspection unchecked
            final NamedList<String> baseSolrUrlList = (NamedList<String>) ((NamedList<?>) plusInitArgs).get("baseSolrUrls");
            final String[] baseSolrUrls = baseSolrUrlList.getAll(null).toArray(new String[baseSolrUrlList.size()]);
            log.warn("baseSolrUrls:{}", Arrays.toString(baseSolrUrls));*/
		this.defaultPageSize = Optional.ofNullable(solrParams.getInt("defaultPageSize"))
				.orElse(this.defaultPageSize);
		this.limitPageNo = Optional.ofNullable(solrParams.getInt("limitPageNo"))
				.orElse(this.limitPageNo);
		this.wt = Optional.ofNullable(solrParams.get(CommonParams.WT)).orElse(this.wt);
		final String baseSolrUrl = Optional.ofNullable(solrParams.get("baseSolrUrl"))
				.orElseThrow(() -> new IllegalArgumentException("baseSolrUrl could not be null."));
//						this.solrClient = createHttpSolrClient(baseSolrUrl);
		this.solrClient = createLBHttpSolrClient(baseSolrUrl);
		this._CACHE_PAGE_TAB = new ArrayList<>(this.limitPageNo);
		this._CACHE_ALL_SERVICE_IDS = new ArrayList<>(this.limitPageNo * this.defaultPageSize);
		log.warn("plus-init args:{}", plusInitArgs.toString());
		//  接口映射器
		this.API_CONSUMER_MAP.put(_API_SELECT_PLUS, handleRequestBody);
		this.API_CONSUMER_MAP.put(_API__CACHE, cacheStat);
	}

	@SuppressWarnings("unused")
	private HttpSolrClient createHttpSolrClient(final String baseSolrUrl) {
		//noinspection UnnecessaryLocalVariable
		final HttpSolrClient solrClient = new HttpSolrClient.Builder()
				.withBaseSolrUrl(baseSolrUrl)
				.build();
		/*try {
			final SolrPingResponse ping = solrClient.ping();
			log.warn("ping status:{}", ping.getStatus());
		} catch (SolrServerException | IOException e) {
			throw new SolrCoreInitializationException(baseSolrUrl.substring(baseSolrUrl.lastIndexOf("/")), e);
		}*/
		return solrClient;
	}

	private LBHttpSolrClient createLBHttpSolrClient(final String... baseSolrUrls) {
		//noinspection UnnecessaryLocalVariable
		final LBHttpSolrClient solrClient = new LBHttpSolrClient.Builder()
				.withBaseSolrUrls(baseSolrUrls)
				.build();
		/*try {
			final SolrPingResponse ping = solrClient.ping();
			log.warn("ping status:{}", ping.getStatus());
		} catch (SolrServerException | IOException e) {
			throw new SolrCoreInitializationException(baseSolrUrl[0].substring(baseSolrUrl[0].lastIndexOf("/")), e);
		}*/
		return solrClient;
	}

	@SuppressWarnings("unused")
	private CloudSolrClient createCloudSolrClient(final String... zkHosts) {
		//noinspection UnnecessaryLocalVariable
		final CloudSolrClient solrClient = new CloudSolrClient.Builder(Arrays.asList(zkHosts), Optional.empty())
				.build();
		/*try {
			final SolrPingResponse ping = solrClient.ping();
			log.warn("ping status:{}", ping.getStatus());
		} catch (SolrServerException | IOException e) {
			throw new SolrCoreInitializationException(baseSolrUrl[0].substring(baseSolrUrl[0].lastIndexOf("/")), e);
		}*/
		return solrClient;
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
//				final SearchHandler searchHandler = (SearchHandler) req.getCore().getRequestHandler("/select");
		final String apiPath = req.getPath();
		log.warn("path:{}", apiPath);
		final BiConsumerPlus<SolrQueryRequest, SolrQueryResponse> consumer = this.API_CONSUMER_MAP.get(req.getPath());
		if (null != consumer){
			consumer.accept(req, rsp);
			return;
		}
		rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + apiPath));
	}

	private final BiConsumerPlus<SolrQueryRequest, SolrQueryResponse> cacheStat = (req, rsp) -> {
		final String httpMethod = req.getHttpMethod();
		if (SolrRequest.METHOD.DELETE.equals(SolrRequest.METHOD.valueOf(httpMethod))) {
			resetCache();
		}
		Map<String, Object> map = new HashMap<>(4);
		map.put("_CACHE_SHOP_ID", _CACHE_SHOP_ID);
		map.put("_CACHE_PAGE_TAB", _CACHE_PAGE_TAB);
		map.put("_CACHE_ALL_SERVICE_IDS", _CACHE_ALL_SERVICE_IDS);
		map.put("all_data_size", _CACHE_ALL_SERVICE_IDS.size());
		rsp.addResponse(map);
	};

	private final BiConsumerPlus<SolrQueryRequest, SolrQueryResponse> handleRequestBody = (req, rsp) -> {
		final SolrParams params = req.getParams();
		if (!(params instanceof MultiMapSolrParams)) {
			ReRankingHandler20220413.this.invokeSuperHandleRequestBody(req, rsp);
			return;
		}
		if (ReRankingHandler20220413.this.semaphore.tryAcquire(2)) {
			ReRankingHandler20220413.this.executor.submit(ReRankingHandler20220413.this.runnable);
		/*executor.submit(new Runnable() {
			private SolrQueryRequest req;

			public Runnable setReq(SolrQueryRequest req) {
				this.req = req;
				return this;
			}

			@Override
			public void run() {
				final String collection = req.getCore().getName();
				try {
					writeCache(collection);
				} catch (IOException | SolrServerException e) {
					log.error("write cache err", e);
					//  标记失败
					FAILED_WRITE_CACHE.set(true);
				}
				semaphore.release();
			}
		}.setReq(req));*/
		}
		final Map<String, String[]> map = ((MultiMapSolrParams) params).getMap();
		final int originalRows = Optional.ofNullable(params.getInt(CommonParams.ROWS)).orElse(ReRankingHandler20220413.this.defaultPageSize);
		final int originalStart = Optional.ofNullable(params.getInt(CommonParams.START)).orElse(0);
		final int index = originalStart / originalRows;
		if (ReRankingHandler20220413.this.limitPageNo < index + 1) {
			ReRankingHandler20220413.this.invokeSuperHandleRequestBody(req, rsp);
			return;
		}
		final int atLeastNeed = originalStart + originalRows;
		while (true) {
			//  缓存未写入或者写入失败
			if ((semaphore.availablePermits() == 1 && FAILED_WRITE_TO_CACHE.get()) || originalStart >= numFound) {
				if (ReRankingHandler20220413.this.numFound != 0) {
					ReRankingHandler20220413.this.invokeSuperHandleRequestBody(req, rsp);
					return;
				}
			}
			final int pageTabSize = ReRankingHandler20220413.this._CACHE_PAGE_TAB.size();
			final int nowTotalNum = ReRankingHandler20220413.this._CACHE_ALL_SERVICE_IDS.size();
			//  缓存数据量不足~1
			if ((index >= pageTabSize || atLeastNeed > nowTotalNum) && ReRankingHandler20220413.this.numFound != nowTotalNum) {
				TimeUnit.MILLISECONDS.sleep(1);
				continue;
			}
			final List<String> serviceIds = new ArrayList<>(originalRows);
			//  TODO originalStart % originalRows == 0 如果不是按分页取，如start不是rows的整数倍时，则截取_CACHE_ALL_SERVICE_IDS；
			//   可能会导致分散失效（原因：同一组rows可能出现同个shopId的不同serviceId，再经过权重规则后就会出现打散失效）
			if (ReRankingHandler20220413.this.defaultPageSize == originalRows && index < pageTabSize && originalStart % originalRows == 0) {
				final List<String> pageContent = ReRankingHandler20220413.this._CACHE_PAGE_TAB.get(index);
				//  TODO:LVZR step-1
				if (originalRows == pageContent.size()) {
					serviceIds.addAll(pageContent);
				}
			}
			//  serviceIds.size() value maybe zero from step-1 && 缓存写入已完成（截取）
			if (serviceIds.isEmpty() /*&& nowTotalNum == this.numFound*/) {
				//  TODO:LVZR step-2
				//  缓存数据量不足~2
				if (failedSubListAndAddAllTo(serviceIds, originalStart, Math.min(atLeastNeed, nowTotalNum))) {
					continue;
				}
				if (serviceIds.size() != originalRows) {
					//  TODO [rest = originalRows - serviceIds.size()] [serviceIds, ..., rest-2, rest-1]
					ReRankingHandler20220413.this.invokeSuperHandleRequestBody(req, rsp);
					return;
				}
			} else if (originalRows != serviceIds.size()) {
				TimeUnit.MILLISECONDS.sleep(1);
				continue;
			}
			final String tmp = serviceIds.toString();
			final Map<String, String[]> paramsMap = new HashMap<>(map);
			paramsMap.remove(CommonParams.START);
			paramsMap.remove(CommonParams.ROWS);
			paramsMap.put(CommonParams.Q, new String[]{String.format("%s:(%s)", SERVICE_ID,
					tmp.substring(1, tmp.length() - 1).replace(",", " "))});
			req.setParams(new MultiMapSolrParams(paramsMap));
			ReRankingHandler20220413.this.invokeSuperHandleRequestBody(req, rsp);
			MultiMapSolrParams.addParam("num_found", String.valueOf(ReRankingHandler20220413.this.numFound), paramsMap);
			MultiMapSolrParams.addParam(CommonParams.WT, ReRankingHandler20220413.this.wt, paramsMap);
			req.setParams(new MultiMapSolrParams(paramsMap));
			return;
		}
	};

	private void invokeSuperHandleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		super.handleRequestBody(req, rsp);
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

	public void filter(Iterator<SolrDocument> solrDocumentIterator) {
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
				_CACHE_PAGE_TAB.get(index).add(serviceId);
			}
			solrDocumentIterator.remove();
			final int tmpSize;
			if ((tmpSize = _CACHE_PAGE_TAB.get(index).size()) > defaultPageSize) {
				log.error("write cache err", new IllegalStateException(String.format(
						"PAGE_TAB's size greater than DEFAULT_PAGE_SIZE (index:%s, %s > %s)", index, tmpSize, defaultPageSize)));
				//  标记失败
				FAILED_WRITE_TO_CACHE.set(true);
				//  清空缓存
				_CACHE_PAGE_TAB.clear();
				_CACHE_SHOP_ID.clear();
				return;
			}
		}
	}

	private void writeCache(@SuppressWarnings("SameParameterValue") @Nullable String collection) throws IOException, SolrServerException {
		final AtomicInteger curPageNo = new AtomicInteger();
		final Map<String, String[]> map = new HashMap<>(2);
		map.put(CommonParams.FL, IDS);
		map.put(CommonParams.ROWS, new String[]{String.valueOf(defaultPageSize)});
		final String[] start = new String[1];
		while (true) {
			final int pageNo = curPageNo.getAndIncrement();
			if (pageNo > limitPageNo) {
				break;
			}
			start[0] = String.valueOf((pageNo) * defaultPageSize);
			map.put(CommonParams.START, start);
			final MultiMapSolrParams multiMapSolrParams = new MultiMapSolrParams(map);
			final QueryResponse query = solrClient.query(collection, multiMapSolrParams);
			this.numFound = query.getResults().getNumFound();
			final Iterator<SolrDocument> solrDocumentIterator = query.getResults().iterator();
			if (!solrDocumentIterator.hasNext()) {
				break;
			}
			filter(solrDocumentIterator);
			if (FAILED_WRITE_TO_CACHE.get()) {
				//  失败返回
				return;
			}
		}
		_CACHE_PAGE_TAB.forEach(_CACHE_ALL_SERVICE_IDS::addAll);
		//  标记成功
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
}
