package org.apache.solr.response;

import java.io.IOException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/4/5.
 */
public class JSONResponseWriterPlus extends JSONResponseWriter {
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void write(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
        final SolrParams params = req.getParams();
	    log.info("JSONResponseWriterPlus.write#SolrParams:{}", params.toQueryString());
        final String wrapperFunction = params.get(JSONWriter.JSON_WRAPPER_FUNCTION);
        final String namedListStyle = params.get(JsonTextWriter.JSON_NL_STYLE, JsonTextWriter.JSON_NL_FLAT).intern();

        final JSONWriter w;
        //  TODO:LVZR
        final String numFound = params.get("num_found");
        if (null != numFound) {
            w = new JSONWriterPlus(writer, req, rsp, wrapperFunction, namedListStyle, Long.parseLong(numFound));
        } else {
            if (namedListStyle.equals(JsonTextWriter.JSON_NL_ARROFNTV)) {
                w = new ArrayOfNameTypeValueJSONWriter(
                        writer, req, rsp, wrapperFunction, namedListStyle, true);
            } else {
                w = new JSONWriter(
                        writer, req, rsp, wrapperFunction, namedListStyle);
            }
        }
        try {
            w.writeResponse();
        } finally {
            w.close();
        }
    }
}
