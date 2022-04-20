package com.lvzr.solr.requesthandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.RawResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.ShowFileRequestHandler.USE_CONTENT_TYPE;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/4/19.
 */
public class DynamicDicRequestHandler extends RequestHandlerBase {

	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final String DIC_FILE_NAME = "dynamicdic.txt";

	@Override
	public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		final Action action = Action.valueOf(Optional.ofNullable(req.getParams().get("action"))
				.orElse(Action.WRITE.toString()));
		switch (action) {
			case READ:
				readDicFile(req, rsp);
				return;
			case WRITE:
				writeDicFile(req, rsp);
				return;
			default:
		}
	}

	private void readDicFile(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
		// Include the file contents
		// The file logic depends on RawResponseWriter, so force its use.
		ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
		params.set(CommonParams.WT, "raw");
		req.setParams(params);

		final CoreContainer coreContainer = req.getCore().getCoreContainer();
		final boolean isZooKeeperAware = coreContainer.isZooKeeperAware();

		ContentStreamBase content = isZooKeeperAware ?
				getDynamicDicFromZooKeeper(req, rsp, coreContainer) : getDynamicDicFromFileSystem(req, rsp);
		if (null == content) {
			return;
		}
		content.setContentType(req.getParams().get(USE_CONTENT_TYPE));

		rsp.add(RawResponseWriter.CONTENT, content);
		rsp.setHttpCaching(false);
	}

	private ContentStreamBase getDynamicDicFromFileSystem(SolrQueryRequest req, SolrQueryResponse rsp) throws FileNotFoundException {
//		// Include the file contents
//		//The file logic depends on RawResponseWriter, so force its use.
//		ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
//		params.set(CommonParams.WT, "raw");
//		req.setParams(params);
		//  ---
		final SolrResourceLoader resourceLoader = req.getCore().getResourceLoader();
		final File adminFile = new File(resourceLoader.getConfigDir(), DIC_FILE_NAME);
		if (!adminFile.exists()) {
			log.error("Can not find: " + adminFile.getName() + " [" + adminFile.getAbsolutePath() + "]");
			rsp.setException(new SolrException
					(SolrException.ErrorCode.NOT_FOUND, "Can not find: " + adminFile.getName()
							+ " [" + adminFile.getAbsolutePath() + "]"));
			return null;
		}
		log.info("FromFileSystem filePath:{}", adminFile.getPath());
		//  ---
		return new ContentStreamBase.FileStream(adminFile);
	}

	private ContentStreamBase getDynamicDicFromZooKeeper(SolrQueryRequest req, SolrQueryResponse rsp, CoreContainer coreContainer)
			throws KeeperException, InterruptedException {
		final SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
		final ZkSolrResourceLoader resourceLoader = (ZkSolrResourceLoader) req.getCore().getResourceLoader();
		final String configSetZkPath = resourceLoader.getConfigSetZkPath();
		final String adminFilePath = configSetZkPath + "/" + DIC_FILE_NAME;
		if (!zkClient.exists(adminFilePath, Boolean.TRUE)) {
			log.error("Can not find: " + adminFilePath);
			rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "Can not find: "
					+ adminFilePath));
			return null;
		}
		log.info("FromZooKeeper filePath:{}", adminFilePath);
		return new ContentStreamBase.ByteArrayStream(zkClient.getData(adminFilePath, null,
				null, Boolean.TRUE), adminFilePath);
	}

	private void writeDicFile(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
		final CoreContainer coreContainer = req.getCore().getCoreContainer();
		final boolean isZooKeeperAware = coreContainer.isZooKeeperAware();
		if (isZooKeeperAware) {
			writeToZookeeper(req, rsp, coreContainer);
		}
	}

	private void writeToZookeeper(SolrQueryRequest req, SolrQueryResponse rsp, CoreContainer coreContainer)
			throws KeeperException, InterruptedException {
		log.info("Start to writeToZookeeper...");
		final SolrZkClient zkClient = coreContainer.getZkController().getZkClient();
		final ZkSolrResourceLoader resourceLoader = (ZkSolrResourceLoader) coreContainer.getResourceLoader();
		final String configSetZkPath = resourceLoader.getConfigSetZkPath();
		final String ikConfFilePath = configSetZkPath + "/ik.conf";
		if (!zkClient.exists(ikConfFilePath, Boolean.TRUE)) {
			log.error("Can not find: " + ikConfFilePath);
			rsp.setException(new SolrException(SolrException.ErrorCode.NOT_FOUND, "Can not find: "
					+ ikConfFilePath));
			return;
		}
		final String dicFilePath = configSetZkPath + "/" + DIC_FILE_NAME;
		//  更新dynamicdic.txt
		zkClient.atomicUpdate(dicFilePath, (oldData) -> testData());
		//  更新ik.conf
		/*final byte[] oldDataBytes = zkClient.getData(ikConfFilePath, null, null, Boolean.TRUE);
		this.updateIkConfFileFuncOld.setParams(oldDataBytes);
		zkClient.atomicUpdate(ikConfFilePath, this.updateIkConfFileFuncOld);*/
		zkClient.atomicUpdate(ikConfFilePath, this.updateIkConfFileFunc);
	}

	private final Function<byte[], byte[]> updateIkConfFileFunc = (oldDataBytes) -> {
		final String originalData = new String(oldDataBytes, StandardCharsets.UTF_8);
		final String keyword = "lastupdate=";
		final int start = originalData.indexOf(keyword) + keyword.length();
		final int oldLen = originalData.length();
		char[] digitalArr = new char[oldLen - start];
		for (int i = start; i < oldLen; i++) {
			final char cr;
			if ((cr = originalData.charAt(i)) < '0' || cr > '9') {
				break;
			}
			digitalArr[i - start] = cr;
		}
		long version = 0;
		final int digitalCnt = digitalArr.length;
		for (int i = 0; i < digitalCnt; i++) {
			final char digital = digitalArr[i];
			//  TODO-LVZR: (int)'0'=48
			version += Math.multiplyExact(((int) digital - (int) '0'), (long) Math.pow(10, digitalCnt - i));
		}
		return originalData.replace(keyword + version,
				keyword + (version + 1)).getBytes(StandardCharsets.UTF_8);
	};

	@Deprecated
	private Function<byte[], byte[]> updateIkConfFileFunc(Object... params) {
		return (oldDataBytes) -> {
			final String originalData = new String(oldDataBytes, StandardCharsets.UTF_8);
			final String keyword = "lastupdate=";
			final int start = originalData.indexOf(keyword) + keyword.length();
			final int oldLen = originalData.length();
			char[] digitalArr = new char[oldLen - start];
			for (int i = start; i < oldLen; i++) {
				final char cr;
				if ((cr = originalData.charAt(i)) < '0' || cr > '9') {
					break;
				}
				digitalArr[i - start] = cr;
			}
			long version = 0;
			final int digitalCnt = digitalArr.length;
			for (int i = 0; i < digitalCnt; i++) {
				final char digital = digitalArr[i];
				//  TODO-LVZR: (int)'0'=48
				version += Math.multiplyExact(((int) digital - (int) '0'), (long) Math.pow(10, digitalCnt - i));
			}
			return originalData.replace(keyword + version,
					keyword + (version + 1)).getBytes(StandardCharsets.UTF_8);
		};
	}

	@Deprecated
	abstract static class FunctionPlus implements Function<byte[], byte[]> {
		protected Object[] params;

		public FunctionPlus() {
		}

		public FunctionPlus(Object[] params) {
			this.params = params;
		}

		protected void setParams(Object... params) {
			this.params = params;
		}
	}

	@Deprecated
	private final FunctionPlus updateIkConfFileFuncOld = new FunctionPlus() {
		@Override
		public byte[] apply(byte[] bytes) {
			final String originalData = new String(bytes, StandardCharsets.UTF_8);
			final String keyword = "lastupdate=";
			final int start = originalData.indexOf(keyword) + keyword.length();
			final int oldLen = originalData.length();
			char[] digitalArr = new char[oldLen - start];
			for (int i = start; i < oldLen; i++) {
				final char cr;
				if ((cr = originalData.charAt(i)) < '0' || cr > '9') {
					break;
				}
				digitalArr[i - start] = cr;
			}
			long version = 0;
			final int digitalCnt = digitalArr.length;
			for (int i = 0; i < digitalCnt; i++) {
				final char digital = digitalArr[i];
				//  TODO-LVZR: (int)'0'=48
				version += Math.multiplyExact(((int) digital - (int) '0'), (long) Math.pow(10, digitalCnt - i));
			}
			return originalData.replace(keyword + version,
					keyword + (version + 1)).getBytes(StandardCharsets.UTF_8);
		}
	};

	/**
	 * TODO-LVZR: for test
	 *
	 * @return test data
	 */
	private byte[] testData() {
		String content = "吕章润" + System.lineSeparator() +
				"章润" + System.lineSeparator() +
				"叶晴" + System.lineSeparator();
		return content.getBytes(StandardCharsets.UTF_8);
	}

	enum Action {
		WRITE, READ
	}

	@Override
	public String getDescription() {
		return null;
	}

	public static void main(String[] args) {
		final String originalData = "Wed Aug 01 00:00:00 CST 2021\n" +
				"files=dynamicdic.txt\n" +
				"lastupdate=234321";
		final String keyword = "lastupdate=";
		final int start = originalData.indexOf(keyword) + keyword.length();
		System.out.println(start);
		System.out.println(originalData.toCharArray()[start]);
		System.out.println((int) '0');
		System.out.println((int) '0');
		System.out.println((int) '9');
		System.out.println((int) '9' - (int) '0');

		final int oldLen = originalData.length();
		char[] digitalArr = new char[oldLen - start];
		for (int i = start; i < oldLen; i++) {
			final char cr;
			if ((cr = originalData.charAt(i)) < '0' || cr > '9') {
				break;
			}
			digitalArr[i - start] = cr;
		}
		int version = 0;
		final int digitalCnt = digitalArr.length;
		for (int i = 0; i < digitalCnt; i++) {
			final char digital = digitalArr[i];
			//  TODO-LVZR: (int)'0'=48
			version += Math.multiplyExact(((int) digital - (int) '0'), (int) Math.pow(10, digitalCnt - i - 1));
		}
		System.out.println("version=" + version);
	}
}
