package org.apache.solr.response;

import java.io.IOException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;

import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lvzr
 * Created by Lvzr on 2022/4/5.
 */
public class JSONWriterPlus extends JSONWriter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final long numFound;

    public JSONWriterPlus(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp, long numFound) {
        super(writer, req, rsp);
        this.numFound = numFound;
    }

    public JSONWriterPlus(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp, String wrapperFunction, String namedListStyle, long numFound) {
        super(writer, req, rsp, wrapperFunction, namedListStyle);
        this.numFound = numFound;
    }

    @Override
    public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore) throws IOException {
        log.warn("numFound：{},this.numFound:{}", numFound, JSONWriterPlus.this.numFound);
        super.writeStartDocumentList(name, start, size, JSONWriterPlus.this.numFound, maxScore);
    }
}
