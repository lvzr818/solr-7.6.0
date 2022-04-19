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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
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
public class ReRankingHandler20220406 extends SearchHandler /*RequestHandlerBase implements PluginInfoInitialized*/ {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final String SERVICE_ID = "service_id";

	private final String SHOP_ID = "shop_id";

	private final String[] IDS = new String[]{SERVICE_ID, SHOP_ID};

	private final ExecutorService executor = Executors.newSingleThreadExecutor();

	private final Semaphore semaphore = new Semaphore(2);

	private final AtomicBoolean FAILED_WRITE_CACHE = new AtomicBoolean();

	private final Map<String, AtomicInteger> SHOP_ID_CACHE = new HashMap<>(1024);

	private List<List<String>> PAGE_TAB;

	private List<String> ALL_DATE_CACHE;

	private volatile long numFound;

	private int defaultPageSize = 40;

	private int limitPageNo = 100;

	private String wt = CommonParams.JSON;

	private SolrClient solrClient;

	private final Runnable runnable = () -> {
		try {
			writeCache(null);
		} catch (IOException | SolrServerException e) {
			log.error("write cache err", e);
			//  标记失败
			FAILED_WRITE_CACHE.set(true);
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
		if (plusInitArgs instanceof NamedList) {
			final SolrParams solrParams = ((NamedList<?>) plusInitArgs).toSolrParams();
			// TODO
//						//noinspection unchecked
//						final NamedList<String> baseSolrUrlList = (NamedList<String>) ((NamedList<?>) plusInitArgs).get("baseSolrUrls");
//						final String[] baseSolrUrls = baseSolrUrlList.getAll(null).toArray(new String[baseSolrUrlList.size()]);
//						log.warn("baseSolrUrls:{}", Arrays.toString(baseSolrUrls));
			this.defaultPageSize = Optional.ofNullable(solrParams.getInt("defaultPageSize"))
					.orElse(this.defaultPageSize);
			this.limitPageNo = Optional.ofNullable(solrParams.getInt("limitPageNo"))
					.orElse(this.limitPageNo);
			this.wt = Optional.ofNullable(solrParams.get(CommonParams.WT)).orElse(this.wt);
			final String baseSolrUrl = Optional.ofNullable(solrParams.get("baseSolrUrl"))
					.orElseThrow(() -> new IllegalArgumentException("baseSolrUrl could not be null."));
//						this.solrClient = createHttpSolrClient(baseSolrUrl);
			this.solrClient = createLBHttpSolrClient(baseSolrUrl);
			this.PAGE_TAB = new ArrayList<>(this.limitPageNo);
			this.ALL_DATE_CACHE = new ArrayList<>(this.limitPageNo * this.defaultPageSize);
			log.warn("plus-init args:{}", plusInitArgs.toString());
		}
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
		log.warn("path:{}", req.getPath());
		final SolrParams params = req.getParams();
		if (!(params instanceof MultiMapSolrParams)) {
			super.handleRequestBody(req, rsp);
			return;
		}
		if (semaphore.tryAcquire(2)) {
			executor.submit(runnable);
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
		final int originalRows = Optional.ofNullable(params.getInt(CommonParams.ROWS)).orElse(defaultPageSize);
		final int originalStart = Optional.ofNullable(params.getInt(CommonParams.START)).orElse(0);
		final int index = originalStart / originalRows;
		if (limitPageNo < index + 1) {
			super.handleRequestBody(req, rsp);
			return;
		}
		final int atLeastNeed = originalStart + originalRows;
		while (true) {
			//  缓存未写入或者写入失败
			if (FAILED_WRITE_CACHE.get() || originalStart >= this.numFound) {
				if (this.numFound != 0) {
					super.handleRequestBody(req, rsp);
					return;
				}
			}
			final int pageTabSize = PAGE_TAB.size();
			final int nowTotalNum = ALL_DATE_CACHE.size();
			//  缓存数据量不足~1
			if ((index >= pageTabSize || atLeastNeed > nowTotalNum) && this.numFound != nowTotalNum) {
				TimeUnit.MILLISECONDS.sleep(1);
				continue;
			}
			final List<String> serviceIds = new ArrayList<>(originalRows);
			if (defaultPageSize == originalRows && index < pageTabSize) {
				final List<String> pageContent = PAGE_TAB.get(index);
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
			paramsMap.remove(CommonParams.ROWS);
			paramsMap.put(CommonParams.Q, new String[]{String.format("%s:(%s)", SERVICE_ID, tmp.substring(1, tmp.length() - 1)
					.replace(",", " "))});
			req.setParams(new MultiMapSolrParams(paramsMap));
			super.handleRequestBody(req, rsp);
			MultiMapSolrParams.addParam("num_found", String.valueOf(numFound), paramsMap);
			MultiMapSolrParams.addParam(CommonParams.WT, this.wt, paramsMap);
			req.setParams(new MultiMapSolrParams(paramsMap));
			return;
		}
	}

	private boolean failedSubListAndAddAllTo(final List<String> serviceIds, final int fromIndex, final int toIndex) {
		final List<String> subList;
		try {
			subList = ALL_DATE_CACHE.subList(fromIndex, toIndex);
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
			final int pageTabSize = PAGE_TAB.size();
			int index = pageTabSize;
			if (SHOP_ID_CACHE.containsKey(shopId)) {
				index = SHOP_ID_CACHE.get(shopId).incrementAndGet();
			} else {
				for (int i = 0; i < pageTabSize; i++) {
					final int size = PAGE_TAB.get(i).size();
					if (size < defaultPageSize) {
						index = i;
						break;
					}
				}
				SHOP_ID_CACHE.put(shopId, new AtomicInteger(index));
			}
			if (index >= pageTabSize) {
				final List<String> set = new ArrayList<>(defaultPageSize);
				set.add(serviceId);
				PAGE_TAB.add(set);
			} else {
				PAGE_TAB.get(index).add(serviceId);
			}
			solrDocumentIterator.remove();
			final int tmpSize;
			if ((tmpSize = PAGE_TAB.get(index).size()) > defaultPageSize) {
				log.error("write cache err", new IllegalStateException(String.format(
						"PAGE_TAB's size greater than DEFAULT_PAGE_SIZE (index:%s, %s > %s)", index, tmpSize, defaultPageSize)));
				//  标记失败
				FAILED_WRITE_CACHE.set(true);
				//  清空缓存
				PAGE_TAB.clear();
				SHOP_ID_CACHE.clear();
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
			if (FAILED_WRITE_CACHE.get()) {
				//  失败返回
				return;
			}
		}
		PAGE_TAB.forEach(ALL_DATE_CACHE::addAll);
		//  标记成功
		FAILED_WRITE_CACHE.set(false);
	}

	private void resetCache() {

	}

	@Override
	public String getDescription() {
		return super.getDescription();
	}
}
