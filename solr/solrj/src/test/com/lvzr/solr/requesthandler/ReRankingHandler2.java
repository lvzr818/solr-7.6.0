package com.lvzr.solr.requesthandler;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/3/31.
 */
public class ReRankingHandler2 extends SearchHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, AtomicInteger> CACHE = new HashMap<>(16);

//  private final Map<String, LinkedHashSet<String>> PAGE_ = new LinkedHashMap<>(16);

  private final List<LinkedHashSet<String>> PAGE_TAB = new ArrayList<>(16);

  private final String SERVICE_ID = "service_id";

  private final String SHOP_ID = "shop_id";

  private final String[] IDS = new String[]{SERVICE_ID, SHOP_ID};

  private final HttpSolrClient solrClient = new HttpSolrClient.Builder()
      .withBaseSolrUrl("http://localhost:8888/solr/test_service")
      .build();

  private final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newSingleThreadExecutor();

  /**
   * TODO: group=true&group.field=shop_id&group.limit=-1
   *
   * @param req ..
   * @param rsp ..
   * @throws Exception ..
   */
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final SolrParams params = req.getParams();
    log.warn("\r\nLVZR-className:{}\r\ntoLocalParamsString:{}\r\ntoString:{}", params.getClass().getName(),
        params.toLocalParamsString(), params.toString());
    if (!(params instanceof MultiMapSolrParams)) {
      super.handleRequestBody(req, rsp);
      return;
    }
    final Map<String, String[]> map = ((MultiMapSolrParams) params).getMap();
    final String[] fl = map.get("fl");
    final int originalRows = Optional.ofNullable(params.getInt("rows")).orElse(10);
    final int originalStart = Optional.ofNullable(params.getInt("start")).orElse(0);
    final int originalPageNo = originalStart / originalRows + 1;
    final AtomicInteger curPageNo = new AtomicInteger(originalPageNo);
    log.warn("LVZR-fl:{}", Arrays.toString(fl));
    while (true) {
//      final LinkedHashSet<String> serviceIds = PAGE_.get(String.valueOf(originalPageNo));
      final int index;
      if ((index = originalPageNo - 1) < PAGE_TAB.size()) {
        final LinkedHashSet<String> serviceIds = PAGE_TAB.get(index);
        if (null != serviceIds && originalRows == serviceIds.size()) {
          final String tmp = serviceIds.toString();
          final Map<String, String[]> paramsMap = new HashMap<>(2);
          paramsMap.put("fl", fl);
          paramsMap.put("q", new String[]{String.format("%s:(%s)", SERVICE_ID, tmp.substring(1, tmp.length() - 1)
              .replace(",", " "))});
          final MultiMapSolrParams multiMapSolrParams = new MultiMapSolrParams(paramsMap);
//          final QueryResponse query = solrClient.query(multiMapSolrParams);
//          rsp.addResponse(query.getResponse());
          req.setParams(multiMapSolrParams);
          super.handleRequestBody(req, rsp);
//          log.warn("response-data-1:{}", query.getResults().size());
          return;
        }
      }
      map.put("fl", IDS);
      map.put("start", new String[]{String.valueOf((curPageNo.get() - 1) * originalRows)});
      final MultiMapSolrParams multiMapSolrParams = new MultiMapSolrParams(map);
      final QueryResponse query = solrClient.query(multiMapSolrParams);
//      final BasicResultContext response = (BasicResultContext) rsp.getResponse();
//      final Iterator<SolrDocument> solrDocumentIterator = response.getProcessedDocuments();
      final Iterator<SolrDocument> solrDocumentIterator = query.getResults().iterator();
      if (!solrDocumentIterator.hasNext()) {
        rsp.addResponse(query.getResponse());
        log.warn("response-data-2:{}", query.getResults().size());
        return;
      }
      filter(solrDocumentIterator, curPageNo.getAndIncrement(), originalRows);
    }
  }

  public void filter(Iterator<SolrDocument> solrDocumentIterator, final int pageNo, final int originalRows) {
//        final String shopId = String.valueOf(doc.getFieldValue(SHOP_ID));
//        final String serviceId = String.valueOf(doc.getFieldValue(SERVICE_ID));
    final List<String> list = new ArrayList<>(originalRows);
    while (solrDocumentIterator.hasNext()) {
      final SolrDocument doc = solrDocumentIterator.next();
//      final String shopId = String.valueOf(Optional.ofNullable(((StoredField) doc.get(SHOP_ID)))
//          .orElse(new StoredField(SHOP_ID, "")).stringValue());
//      final String serviceId = String.valueOf(Optional.ofNullable(((StoredField) doc.get(SERVICE_ID)))
//          .orElse(new StoredField(SERVICE_ID, "")).stringValue());
      final String shopId = String.valueOf(Optional.ofNullable(doc.get(SHOP_ID))
          .orElse(""));
      final String serviceId = String.valueOf(Optional.ofNullable(doc.get(SERVICE_ID))
          .orElse(""));
      list.add(shopId + "#" + serviceId);
      final int pageTabSize = PAGE_TAB.size();
      int index = pageNo;
      if (CACHE.containsKey(shopId)) {
        index = CACHE.get(shopId).incrementAndGet();
      } else {
        /*for (Map.Entry<String, LinkedHashSet<String>> entry : PAGE_.entrySet()) {
          final String page = entry.getKey();
          final int size = entry.getValue().size();
          if (size < originalRows) {
            index = Integer.parseInt(page);
            break;
          }
        }*/
        for (int i = 0; i < pageTabSize; i++) {
          final int size = PAGE_TAB.get(i).size();
          if (size < originalRows) {
            index = i;
            break;
          }
        }
        CACHE.put(shopId, new AtomicInteger(index));
      }
//      PAGE_.computeIfAbsent(String.valueOf(index), (k) -> new LinkedHashSet<>()).add(serviceId);
      if (index >= pageTabSize) {
        final LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add(serviceId);
        PAGE_TAB.add(set);
      } else {
        PAGE_TAB.get(index).add(serviceId);
      }
//      PAGE_TAB.computeIfAbsent(String.valueOf(index), (k) -> new LinkedHashSet<>()).add(serviceId);
      solrDocumentIterator.remove();
    }
    log.warn("pageNo:{}, list:{}", pageNo, list);
  }


//    /////////////////////////////////////////

    /*@Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        // 获取查询参数（在 solr-console log中可以看到）
        System.out.println(req.getParams().get("q"));
        // 设置返回体
        Map<String, Object> result = new HashMap<>();
        result.put("name", "Tom");
        result.put("label", "Jack");
        rsp.addResponse(result);
    }

    @Override
    public String getDescription() {
        return null;
    }*/
}
