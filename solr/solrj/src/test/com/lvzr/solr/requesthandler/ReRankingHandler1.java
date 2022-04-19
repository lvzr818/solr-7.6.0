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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/3/31.
 */
public class ReRankingHandler1 extends SearchHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, AtomicInteger> CACHE = new HashMap<>(16);

  private final Map<String, LinkedHashSet<String>> PAGE_ = new HashMap<>(16);

  private final String SERVICE_ID = "service_id";

  private final String SHOP_ID = "shop_id";

  private final String[] IDS = new String[]{SERVICE_ID, SHOP_ID};

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
    final int originalRow = Optional.ofNullable(params.getInt("rows")).orElse(10);
    final int originalStart = Optional.ofNullable(params.getInt("start")).orElse(0);
    final int originalPageNo = originalStart / originalRow + 1;
    final AtomicInteger curPageNo = new AtomicInteger(originalPageNo);
    log.warn("LVZR-fl:{}", Arrays.toString(fl));
    while (true) {
      final LinkedHashSet<String> serviceIds = PAGE_.get(String.valueOf(originalPageNo));
      if (null != serviceIds && originalRow == serviceIds.size()) {
        final String tmp = serviceIds.toString();
        final Map<String, String[]> paramsMap = new HashMap<>(2);
        paramsMap.put("fl", fl);
        paramsMap.put("q", new String[]{tmp.substring(1, tmp.length() - 1)
            .replace(",", " ")});
        final MultiMapSolrParams multiMapSolrParams = new MultiMapSolrParams(paramsMap);
        req.setParams(multiMapSolrParams);
        super.handleRequestBody(req, rsp);
//        log.info("stat-cache:{};page_:{}", JSONUtil.toJsonPrettyStr(CACHE), JSONUtil.toJsonPrettyStr(PAGE_));
        return;
      }
      map.put("fl", IDS);
      map.put("start", new String[]{String.valueOf((curPageNo.get() - 1) * originalRow)});
      map.put("_", new String[]{String.valueOf(System.currentTimeMillis())});
      final MultiMapSolrParams multiMapSolrParams = new MultiMapSolrParams(map);
      req.setParams(multiMapSolrParams);
      super.handleRequestBody(req, rsp);
      final BasicResultContext response = (BasicResultContext) rsp.getResponse();
      final Iterator<SolrDocument> solrDocumentIterator = response.getProcessedDocuments();
      if (!solrDocumentIterator.hasNext()) {
        return;
      }
      filter(solrDocumentIterator, curPageNo.getAndIncrement(), originalRow);
      rsp = new SolrQueryResponse();
    }
  }

  public void filter(Iterator<SolrDocument> solrDocumentIterator, final int pageNo, final int originalRow) {
//        final String shopId = String.valueOf(doc.getFieldValue(SHOP_ID));
//        final String serviceId = String.valueOf(doc.getFieldValue(SERVICE_ID));
    final List<SolrDocument> list = new ArrayList<>(originalRow);
    while (solrDocumentIterator.hasNext()) {
      final SolrDocument doc = solrDocumentIterator.next();
      list.add(doc);
      final String shopId = String.valueOf(doc.getFieldValue(SHOP_ID));
      final String serviceId = String.valueOf(doc.getFieldValue(SERVICE_ID));
      int index = pageNo;
      if (CACHE.containsKey(shopId)) {
        final int nextPageNo = CACHE.get(shopId).incrementAndGet();
//        index = Math.max(nextPageNo, pageNo);
        index = nextPageNo;
      } else {
        for (Map.Entry<String, LinkedHashSet<String>> entry : PAGE_.entrySet()) {
          final String page = entry.getKey();
          final int size = entry.getValue().size();
          if (size < originalRow) {
            index = Integer.parseInt(page);
            break;
          }
        }
//        index = Math.max(1, pageNo);
        CACHE.put(shopId, new AtomicInteger(index));
      }
      PAGE_.computeIfAbsent(String.valueOf(index), (k) -> new LinkedHashSet<>()).add(serviceId);
      solrDocumentIterator.remove();
    }
    log.warn("pageNo:{},list:{}", pageNo, list);
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
