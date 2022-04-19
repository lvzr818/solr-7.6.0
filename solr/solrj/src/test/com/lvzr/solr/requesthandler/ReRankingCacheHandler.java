/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lvzr.solr.requesthandler;

import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/4/7.
 * <p>
 * solr里不同的path对应不同的RequestHandler实例对象；
 * 如("/select","/query")虽然配置的Handler均是SearchHandler，但是是各自两个不同的实例对象
 */
public class ReRankingCacheHandler extends RequestHandlerBase {
	@Override
	public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		final ReRankingHandler requestHandler = (ReRankingHandler) req.getCore().getRequestHandler(ReRankingHandler._API_SELECT_PLUS);
		if (null != requestHandler) {
			requestHandler.handleRequestBody(req, rsp);
			return;
		}
		rsp.setException(new RuntimeException("error"));
	}

	@Override
	public String getDescription() {
		return null;
	}
}
