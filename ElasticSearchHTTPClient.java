package com.aol.identity.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 9/23/13
 * Time: 3:42 PM
 * To change this template use File | Settings | File Templates.
 */

import com.aol.identity.util.HttpApiCall;
import com.aol.interfaces.user.user_service_search_types.v2.SearchOperator;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.jackson.JacksonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticSearchHTTPClient<T extends Indexable> implements Search<T> {

    private final static Logger LOG = LoggerFactory.getLogger(ElasticSearchHTTPClient.class);
    private final static Logger JSON_LOG = LoggerFactory.getLogger(ElasticSearchHTTPClient.class.getName() + ".Json");

    public static class ElasticSearchHTTPClientException extends RuntimeException {
        public ElasticSearchHTTPClientException(String reason) {
            super(reason);
        }
    }

    private static final String SEARCH_PROPS_NS = "com.aol.search.es.";

    public static final String SEARCH_PROPS_IDX_NAME = SEARCH_PROPS_NS + "index.name";
    public static final String SEARCH_PROPS_TYPE_NAME = SEARCH_PROPS_NS + "type.name";
    public static final String SEARCH_PROPS_IDX_SHARDS = SEARCH_PROPS_NS + "index.shards";
    public static final String SEARCH_PROPS_IDX_REPLICAS = SEARCH_PROPS_NS + "index.replicas";
    public static final String SEARCH_PROPS_READ_TIMEOUT = SEARCH_PROPS_NS + "request.timeout.read";
    public static final String SEARCH_PROPS_CONNECT_TIMEOUT = SEARCH_PROPS_NS + "request.timeout.connect";
    public static final String SEARCH_PROPS_REQUEST_TIMEOUT = SEARCH_PROPS_NS + "request.timeout";
    public static final String SEARCH_PROPS_URL = SEARCH_PROPS_NS + "http.urls";

    private static final Integer LIMIT_DEFAULT = 50;
    private static final Integer LIMIT_MAX = 1000;

    private static final String LOG_REQ_TAG = "Request";
    private static final String LOG_RES_TAG = "Response";
    private static final JsonFactory JsonFactory = new JacksonFactory();
    private static final Executor Executor = Executors.newCachedThreadPool();


    private Integer readTimeout;
    private Integer connectTimeout;
    private String fallbackTimeout;
    private String index;
    private String type;
    private String[] urls = new String[1];
    private AtomicInteger urlIndex = new AtomicInteger(0);
    private PostActivity[] dosAfter = new PostActivity[] {};
    private final Properties properties;


    final private HttpApiCall apiCall;


    public ElasticSearchHTTPClient(Properties props) {

        fallbackTimeout = props.getProperty(SEARCH_PROPS_REQUEST_TIMEOUT, "1000");
        readTimeout = Integer.valueOf(props.getProperty(SEARCH_PROPS_READ_TIMEOUT, fallbackTimeout));
        connectTimeout = Integer.valueOf(props.getProperty(SEARCH_PROPS_CONNECT_TIMEOUT, fallbackTimeout));


        index = props.getProperty(SEARCH_PROPS_IDX_NAME);
        type = props.getProperty(SEARCH_PROPS_TYPE_NAME);
        urls[0] = props.getProperty(SEARCH_PROPS_URL);

        if (index == null || type == null || urls[0] == null) {
            throw new ElasticSearchHTTPClientException("null index name, type or urls.");
        }

        urls = urls[0].split(",");

        for (int i = 0; i < urls.length; i++) {

            String url = urls[i];
            if (!url.startsWith("http")) {
                throw new ElasticSearchHTTPClientException("Invalid url " + url);
            }

            if (!('/' == url.charAt(url.length() - 1))) {
                urls[i] = url + "/";
            }
        }


        apiCall = new HttpApiCall(connectTimeout, readTimeout);

        LOG.info("Read Timeout {}", readTimeout);
        LOG.info("Connect Timeout {}", connectTimeout);
        LOG.info("Index {}/{}", index, type);

        for (int i = 0; i < urls.length; i++) {
            LOG.info("Cluster Url #{} {}", i + 1, urls[i]);
        }

        if (!indexExists(index)) {
            LOG.info("About to set up the {} index for the {} type.", index, type);
            doFirstTimeSetup(props);
            LOG.info("Setup done for the {} index and the {} type.", index, type);
        }

        this.properties = props;
    }

    public void postProcessSetup(PostActivity... dosAfter) {
        if (dosAfter != null)
            this.dosAfter = dosAfter;
    }

    private void doFirstTimeSetup(Properties props) {



        boolean success = false;

        try {

            int numShards = Integer.valueOf(props.getProperty(SEARCH_PROPS_IDX_SHARDS, "10"));
            int replicas = Integer.valueOf(props.getProperty(SEARCH_PROPS_IDX_REPLICAS, "2"));

            Map<String, ?> template = populateTypeSourceWithDefaults(numShards, replicas);
            Map<String, ?> settings = getDefaultIndexAnalysis();

            HttpApiCall c = getHttpInstance();
            String url = getNextUrlAsString(null);
            String templateEndPoint = url + "_template/" + index;
            String settingsEndPoint = url + index;

            success = c.putJson(templateEndPoint, template, null, ResultResponse.class).getOk()
                   && c.putJson(settingsEndPoint, settings, null, ResultResponse.class).getOk();

        } catch (IOException e) {

            LOG.error("SETUP-EX-IO for {}/{}", new String[]{index, type});

        } catch (NumberFormatException e) {

            throw new ElasticSearchHTTPClientException("First time config cannot happen without " +
                    "a specified number of shards and replica factor.\n" +
                    "Please set the " + SEARCH_PROPS_IDX_SHARDS + "property" + "\n" +
                    "And set the " + SEARCH_PROPS_IDX_SHARDS + "property appropriately.");

        }

        if (!success) {
            throw new ElasticSearchHTTPClientException("First time config attempt failed.");
        }
    }


    private Map<String, Object> getDefaultIndexAnalysis() {

        //Default Analysis.
        return new TreeMap<String, Object>() {{

            put("analysis", Collections.singletonMap("analyzer",
                    Collections.singletonMap("default", new HashMap<String, String>() {{
                        put("type", "custom");
                        put("filter", "lowercase");
                        put("tokenizer", "keyword");
                    }})));

        }};
    }

    private Map<String, ?> populateTypeSourceWithDefaults(int shards, int replicas) {

        final Map<String, Object> typeSrc = new TreeMap<String, Object>();

        typeSrc.put("template", index);
        typeSrc.put("order", 0);

        Map<String, Object> settings = new HashMap<String, Object>();
        settings.put("number_of_shards", shards);
        settings.put("number_of_replicas", replicas);
        typeSrc.put("settings", settings);

        Map<String, Object> index = new HashMap<String, Object>();
        index.putAll(getDefaultIndexAnalysis());
        typeSrc.put("index", index);

        Map<String, Object> properties = new HashMap<String, Object>();
        Map<String, Object> all = new HashMap<String, Object>();
        Map<String, Object> source = new HashMap<String, Object>();
        Map<String, Object> mapping = new HashMap<String, Object>();
        List<Map<String, Object>> dynTemplatesList = new ArrayList<Map<String, Object>>();
        Map<String, Object> baseMappingTemplate = new HashMap<String, Object>();
        Map<String, Object> typeMapping = new HashMap<String, Object>();
        Map<String, Object> mappings = new HashMap<String, Object>();

        typeMapping.put("properties", properties);
        all.put("enabled", false);
        typeMapping.put("_all", all);
        source.put("enabled", true);
        source.put("compress", true);
        source.put("excludes", new ArrayList<String>() {
            {
                add("__*");
                add("_id");
            }

            private static final long serialVersionUID = 1L;
        });
        typeMapping.put("_source", source);
        mapping.put("type", "string");
        mapping.put("index", "analyzed");

        baseMappingTemplate.put("match", "*");
        baseMappingTemplate.put("match_mapping_type", "string");
        baseMappingTemplate.put("mapping", mapping);
        dynTemplatesList.add(new HashMap<String, Object>());
        dynTemplatesList.get(0).put("baseMappingTemplate", baseMappingTemplate);
        typeMapping.put("dynamic_templates", dynTemplatesList);
        mappings.put(type, typeMapping);
        typeSrc.put("mappings", mappings);

        return typeSrc;
    }

    private String getNextUrlAsString(String api) {

        //@mea: Lock free, eventually it will rewind to 0.
        //Max offset of overflow will be length - 1
        urlIndex.compareAndSet(urls.length, 0);
        String url = urls[urlIndex.getAndIncrement() % urls.length] + (api == null ? "" : index + "/" + type + "/" + api);
        LOG.debug("NEXT_URL {} {} ", url, urlIndex);

        return url;

    }


    private Map<String, ?> buildFilterClauses(Map<String, ?> constrains, final Map<String, ?> query) {


        if (constrains == null || constrains.isEmpty()) return query;


        Map<String, ?> filtered = Collections.singletonMap("filtered", new TreeMap<String, Object>() {{
            putAll(query);
            put("filter", Collections.singletonMap("and", new ArrayList<Map>()));
        }});

        List<Map> filters = (List) (((Map) ((Map) filtered.get("filtered")).get("filter")).get("and"));


        if (constrains.containsKey("exists")) {

            List<Term> existsTerms = (List<Term>) constrains.get("exists");


            for (final Term existsTerm : existsTerms) {

                Map filterElem = Collections.singletonMap("exists", Collections.singletonMap("field", existsTerm.getName()));

                if (existsTerm.isRelation()) {


                    Map<String, ? super Object> nested = new TreeMap<String, Object>();
                    nested.put("path", existsTerm.getPath());
                    nested.put("filter", filterElem);
                    filterElem = Collections.singletonMap("nested", nested);
                }

                filters.add(filterElem);
            }
        }

        Map<String, ? super Object> ret = new TreeMap<String, Object>();
        Map innerFiltered = (Map) filtered.get("filtered");
        ret.put("query", filtered);
        ret.put("from", innerFiltered.remove("from"));
        ret.put("size", innerFiltered.remove("size"));

        return ret;
    }


    private HttpApiCall getHttpInstance() {
        return apiCall;
    }

    @Override
    public boolean indexExists(String idxName) {

        HttpApiCall c = getHttpInstance();

        try {
            HttpRequestFactory factory = c.createFactory(null, null);
            HttpRequest request = factory.buildHeadRequest(new GenericUrl(getNextUrlAsString(null) + index));
            HttpResponse response = request.execute();
            return response.getStatusCode() == 200;

        } catch (IOException e) {
            LOG.error("IDX-EX-IO for {} threw IOException {}", idxName, e.getMessage());
        }
        return false;

    }

    @Override
    public boolean registerRelation(String elementName) {

        if (elementName == null) {
            throw new ElasticSearchHTTPClientException("Cannot register a null relation");
        }


        Map<String, String> nested = Collections.singletonMap("type", "nested");
        Map<String, ?> element = Collections.singletonMap(elementName, nested);
        Map<String, ?> properties = Collections.singletonMap("properties", element);
        Map<String, ?> mapping = Collections.singletonMap("nested_" + elementName, properties);

        HttpApiCall c = getHttpInstance();

        ESContext.Builder builder = new ESContext.Builder();
        ESContext ctx = builder
                .action(Action.RegRel)
                .logger(LOG)
                .start(System.currentTimeMillis())
                .build();

        try {

            return c.putJson(getNextUrlAsString("_mapping"), mapping, null, ResultResponse.class)
                    .getOk();

        } catch (IOException e) {
            LOG.error("REGREL-EX-IO for {}", new String[]{elementName});
            return false;
        } finally {
            for (PostActivity a : dosAfter) {
                a.postProcess(ctx);
            }
        }

    }

    @Override
    public boolean upsert(T indexee) {

        if (indexee == null || indexee.getId() == null) {
            throw new ElasticSearchHTTPClientException("indexee is null.");
        }

        HttpApiCall c = getHttpInstance();

        ESContext.Builder builder = new ESContext.Builder();
        ESContext ctx = builder
                .action(Action.Upsert)
                .logger(LOG)
                .start(System.currentTimeMillis())
                .build();

        try {
            debugLogJsonObject(indexee.toMap(), LOG_REQ_TAG, Action.Upsert);
            ResultResponse r = c.putJson(getNextUrlAsString("") + indexee.getId(), indexee.toMap(), null, ResultResponse.class);
            debugLogJsonObject(r, LOG_RES_TAG, Action.Upsert);
            return r.getOk();


        } catch (IOException e) {
            LOG.error("UPSERT-EX-IO for {}/{}/{}", new String[]{index, type, indexee.getId()});
            return false;
        } finally {
            for (PostActivity a : dosAfter) {
                a.postProcess(ctx);
            }
        }

    }

    @Override
    public boolean remove(String _id) {
        if (_id == null || _id.isEmpty()) {
            throw new ElasticSearchHTTPClientException("id is empty/null.");
        }

        HttpApiCall c = getHttpInstance();

        ESContext.Builder builder = new ESContext.Builder();
        ESContext ctx = builder
                .action(Action.Remove)
                .logger(LOG)
                .start(System.currentTimeMillis())
                .build();

        try {

            ResultResponse r = c.deleteJson(getNextUrlAsString("") + _id, null, ResultResponse.class);
            debugLogJsonObject(r, LOG_RES_TAG, Action.Remove);
            return r.getOk();


        } catch (IOException e) {
            LOG.error("REMOVE-EX-IO for {}/{}/{}", new String[]{index, type, _id});
            return false;
        } finally {
            for (PostActivity a : dosAfter) {
                a.postProcess(ctx);
            }
        }


    }

    @Override
    public void asyncUpsert(final T indexee) {
        Executor.execute(new Runnable() {
            @Override
            public void run() {
                ;upsert(indexee);
            }
        });
    }

    @Override
    public void asyncRemove(final String _id) {
        Executor.execute(new Runnable() {
            @Override
            public void run() {
                remove(_id);
            }
        });
    }

    @Override
    public long countEqual(final Term term) {


        if (null == term || null == term.getName() || null == term.getValue()) {
            throw new ElasticSearchHTTPClientException("queryTerm, queryValue cannot be null.");
        }


        Map<String, ?> q = Collections.singletonMap("term", Collections.singletonMap(
                term.getName(),
                term.getValue().toLowerCase()));

        if (term.isRelation()) {

            final Map<String, ?> innerQ = q;
            q = Collections.singletonMap("nested", new TreeMap<String, Object>() {{
                put("path", term.getPath());
                put("query", innerQ);
            }});
        }

        HttpApiCall c = getHttpInstance();

        ESContext.Builder builder = new ESContext.Builder();
        ESContext ctx = builder
                .action(Action.Count)
                .logger(LOG)
                .start(System.currentTimeMillis())
                .build();
        try {

            debugLogJsonObject(q, LOG_REQ_TAG, Action.Count);
            CountResponse r = c.postJson(getNextUrlAsString("_count"), q, null, CountResponse.class);
            debugLogJsonObject(r, LOG_RES_TAG, Action.Count);

            return  r.getCount();

        } catch (IOException e) {
            LOG.error("COUNT-EX-IO for {}/{}/[{}]", new String[]{index, type, term.toString()});
            return -1L;

        } finally {

            for (PostActivity a : dosAfter) {
                a.postProcess(ctx);
            }
        }
    }

    @Override
    public List<T> lookup(List<Term> terms, Integer limit, Map<String, ?> constrains, Class<T> impl) {

        if (null == terms || terms.isEmpty()) {
            throw new ElasticSearchHTTPClientException("the lookup needs at least one valid term");
        }


        final int effectiveLimit = (limit == null ? LIMIT_DEFAULT : Math.min(limit, LIMIT_MAX));
        final Collection<Map> musts = new ArrayList<Map>();
        final Collection<Map> shoulds = new ArrayList<Map>();

        for (final Term t : terms) {

            Map<String, ?> innerQuery = Collections.singletonMap("wildcard", Collections.singletonMap(t.getName(), t.getValue().toLowerCase()));

            if (t.isRelation()) {
                final Map<String, ?> effectiveInnerQuery = innerQuery;

                innerQuery = Collections.singletonMap("nested",
                        new TreeMap<String, Object>() {{
                            put("query", effectiveInnerQuery);
                            put("path", t.getPath());
                        }});
            }

            if (SearchOperator.AND.equals(t.getOperator())) {
                musts.add(innerQuery);
            } else if (SearchOperator.OR.equals(t.getOperator())) {
                shoulds.add(innerQuery);
            }
        }


        Map<String, ?> request = new TreeMap<String, Object>() {{
            put("size", effectiveLimit);
            put("from", 0);
            put("query", Collections.singletonMap("bool", new TreeMap<String, Object>() {{
                put("must", musts);
                put("should", shoulds);
                put("minimum_should_match", shoulds.size() > 1 ? 1 : 0);
            }}));
        }};

        request = buildFilterClauses(constrains, request);

        HttpApiCall c = getHttpInstance();

        ESContext.Builder builder = new ESContext.Builder();
        ESContext ctx = builder
                .action(Action.Lookup)
                .logger(LOG)
                .start(System.currentTimeMillis())
                .build();

        try {

            debugLogJsonObject(request, LOG_REQ_TAG, Action.Lookup);
            LookupResponse response = c.postJson(getNextUrlAsString("_search"), request, null, LookupResponse.class);
            debugLogJsonObject(response, LOG_RES_TAG, Action.Lookup);
            //ES limits are per shard. We will specify a cut-over once the limit is hit.
            List<Map.Entry<String, Map<String, ?>>> sources = response.getResults(limit);



            if (response.getTimedOut()) {
                LOG.warn("ES-LOOKUP-TIMEOUT taking {}", response.getTook());
            }

            List<T> results = new ArrayList<T>();

            for (Map.Entry<String, Map<String, ?>> item : sources) {

                Indexable oneResult = impl.newInstance();
                oneResult.fromMap(item.getKey(), (Map) item.getValue());
                results.add((T) oneResult);
            }

            return results;


        } catch (IOException e) {
            LOG.error("LOOKUP-EX-IO for {}/{}/[{} ... {}]", new String[]{index, type,
                    terms.get(0).toString(), terms.get(terms.size() - 1).toString()});
        } catch (InstantiationException e) {
            LOG.error("LOOKUP-InstantiationEx for {}/{}/[{} ... {}]", new String[]{index, type,
                    terms.get(0).toString(), terms.get(terms.size() - 1).toString()});
        } catch (IllegalAccessException e) {
            LOG.error("LOOKUP-IllegalAccessEx for {}/{}/[{} ... {}]", new String[]{index, type,
                    terms.get(0).toString(), terms.get(terms.size() - 1).toString()});
        } finally {

            for (PostActivity a : dosAfter) {
               a.postProcess(ctx);
            }

        }

        return null;
    }

    private static void debugLogJsonObject(Object o, String logTag, Action a) throws IOException {

        if (JSON_LOG.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            JsonGenerator g = JsonFactory.createJsonGenerator(sw);
            g.enablePrettyPrint();
            g.serialize(o);
            g.flush();

            JSON_LOG.debug("Search {} {} \n{}", new Object[]{a, logTag, sw});
        }
    }

    public Properties usedProperties() {
        return properties;
    }
}
