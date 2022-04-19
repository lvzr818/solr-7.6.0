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
package org.eclipse.jetty.servlet;

import javax.servlet.Filter;
import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since solr 1.3
 */
public class StartSolrJetty {

	private final static Logger log = LoggerFactory.getLogger(StartSolrJetty.class);

	static {
//		-Dlog4j.configurationFile=file:///E:/OpenSource/solr-7.6.0/server/resources/log4j2.xml -Dsolr.log.dir=E:\\OpenSource\\solr-7.6.0\\solr_logs
		System.setProperty("solr.install.dir", "E:\\OpenSource\\zip\\solr-7.6.0");
		System.setProperty("solr.solr.home", "E:\\epwk-storage\\dev\\solr_home\\7_6_0");
//		System.setProperty("solr.log.dir", "E:\\OpenSource\\solr-7.6.0\\solr_logs");
//		final LoggerContext context = (LoggerContext) LogManager.getContext(false);
//		final File file = new File("E:\\OpenSource\\solr-7.6.0\\server\\resources\\log4j2.xml");
//		context.setConfigLocation(file.toURI());
//		context.reconfigure();
	}

	public static void main(String[] args) {
//    final String path = StartSolrJetty.class.getResource("/").getPath();
//    System.out.println(path);
//    System.out.println(System.getProperty("user.dir"));
//		System.setProperty("solr.install.dir", "E:\\OpenSource\\zip\\solr-7.6.0");
//		System.setProperty("solr.solr.home", "E:\\epwk-storage\\dev\\solr_home\\7_6_0");
//		System.setProperty("solr.log.dir", "E:\\OpenSource\\solr-7.6.0\\solr_logs");
//    System.setProperty("solr.solr.home", "../../../example/solr");

		Server server = new Server();
		ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory());
		// Set some timeout options to make debugging easier.
		connector.setIdleTimeout(1000 * 60 * 60);
		connector.setPort(8888);
		server.setConnectors(new Connector[]{connector});

		WebAppContext bb = new WebAppContext();
		bb.setServer(server);
		bb.setContextPath("/solr");
		bb.setWar(System.getProperty("user.dir") + "/solr/webapp/web");

//    // START JMX SERVER
//    if( true ) {
//      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
//      MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
//      server.getContainer().addEventListener(mBeanContainer);
//      mBeanContainer.start();
//    }

		server.setHandler(bb);

		try {
			System.out.println(">>> STARTING EMBEDDED JETTY SERVER, PRESS ANY KEY TO STOP");
			server.start();
			while (System.in.available() == 0) {
				Thread.sleep(5000);
			}
			//  TODO
//      test(bb);
			server.stop();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(100);
		}
	}

	private static void test(WebAppContext bb) {
//    final FilterRegistration solrRequestFilter = bb.getServletContext().getFilterRegistration("SolrRequestFilter");
//    final FilterHolder.Registration registration = (FilterHolder.Registration)solrRequestFilter;
//    System.out.println(registration);
		final Filter filter = bb.getServletHandler().getFilter("SolrRequestFilter").getFilter();
		if (filter instanceof SolrDispatchFilter) {
			final CoreContainer cores = ((SolrDispatchFilter) filter).getCores();
			final SolrCore solrCore = cores.getCore("test_service");
			final PluginBag<SolrRequestHandler> requestHandlers = solrCore.getRequestHandlers();
			//  url and Handler mappings
			final SolrRequestHandler requestHandler = solrCore.getRequestHandler("/select");
			System.out.println(requestHandlers);
		}
	}
}
