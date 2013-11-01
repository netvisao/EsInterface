package com.a.identity.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 9/24/13
 * Time: 11:03 AM
 * To change this template use File | Settings | File Templates.
 */

import com.google.api.client.util.Key;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LookupResponse<T> {

    //{"took":2290,"timed_out":false,"_shards":{"total":40,"successful":40,"failed":0},"hits":{"total":0,"max_score":null,"hits":[]}}
    private static final List<Hits.Result> EmptyResults = Collections.emptyList();

    public LookupResponse() {

    }


    private final static Logger LOG = LoggerFactory.getLogger(LookupResponse.class);

    @Key
    private int took;

    @Key("timed_out")
    private Boolean timedOut;

    @Key
    private Hits hits;


    public int getTook() {
        return took;
    }

    public void setTook(int took) {
        this.took = took;
    }

    public Boolean getTimedOut() {
        return timedOut == null ? false : timedOut;
    }

    public void setTimedOut(Boolean timedOut) {
        this.timedOut = timedOut;
    }

    public Hits getHits() {
        return hits;
    }

    public void setHits(Hits hits) {
        this.hits = hits;
    }

    public List<Map.Entry<String,Map<String, ?>>> getResults(final Integer limit) {

        List<Hits.Result> results = getHits() == null || getHits().getResults() == null
                ? EmptyResults
                : getHits().getResults();

        List<Map.Entry<String,Map<String, ?>>> sources = new ArrayList<Map.Entry<String,Map<String, ?>>>();

        int count = 0;
        int stop = (limit == null) ? results.size() : limit;

        for (Hits.Result result : results) {

            if (++count > stop) {
                LOG.info("LIMIT_HIT");
                break;
            }

            if (result != null && result.getSource() != null) {
                sources.add(
                        new AbstractMap.SimpleImmutableEntry<String, Map<String, ?>>(
                                result.getId(),
                                result.getSource()
                        )
                );
            } else {
                LOG.error("UNEXPECTED-NULL-RESULT Id {} has a null result", result.getId());
            }
        }

        return sources;
    }

    public  List<Map.Entry<String,Map<String, ?>>> getResults() {
        return getResults(null);
    }
}
