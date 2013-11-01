package com.aol.identity.search;

import com.aol.interfaces.user.user_service_search_types.v2.SearchOperator;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchTimeoutException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.loader.YamlSettingsLoader;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.query.*;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.existsFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


@Deprecated
//Use ElasticSearchHTTPClient instead.
public final class EmbeddedESClientNode<T extends Indexable> implements Closeable, Search<T> {

    public static class EmbeddedESClientNodeRuntimeException extends RuntimeException {
        public EmbeddedESClientNodeRuntimeException(String reason, Throwable cause) {
            super(reason, cause);
        }
        public EmbeddedESClientNodeRuntimeException(String reason) {
            super(reason);
        }
    }

	private static final String SEARCH_DEFAULT_IDX_NAME         = "index";
	private static final String SEARCH_DEFAULT_TYPE_NAME        = "gentype";
	private static final String SEARCH_PROPS_NS                 = "com.aol.search.es.";
	private static final String SEARCH_PROPS_CFG_PATH           = "path.conf";
	private static final String SEARCH_PROPS_IDX_NAME           = "index.name";
	private static final String SEARCH_PROPS_TYPE_NAME          = "type.name";
    private static final String SEARCH_PROPS_NODE_NAME          = "node.name";
	private static final String SEARCH_PROPS_SHARDS_NUM         = "shards.num";
    private static final String SEARCH_PROPS_REQUEST_TIMEOUTMS  = "request.timeout";
    private static final String SEARCH_PROPS_ASYNC_POOL_SIZE    = "async.threadpool.size";
    private static final String SEARCH_PROPS_ASYNC_QUEUE_SIZE   = "async.queue.size";
    public final static  String SEARCH_POOL_TIMEOUT_MS          = "async.threadpool.timeout";


    private ThreadPoolExecutor searchThreadPool;

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedESClientNode.class);
    private String typeName;
	private String idxName;
	private Client client = null;
	private Node node = null;
    private Properties configProps;
    private TimeValue requestTimeOutMs = null;
    private volatile boolean initialized = false;


    public EmbeddedESClientNode(Properties configProps) {
        this.configProps = configProps;

        if (null == configProps) {
            throw new EmbeddedESClientNodeRuntimeException("Search will not initialize properly, provided properties are null.");
        }

        init();
    }

    private boolean isReady() {

        boolean isReady = client != null && initialized;
        return isReady;
    }

    private void init() {
        LOG.info("EmbeddedESClientNode initializing.");

        Map<String, String> esProps = new HashMap<String, String>();
        for (Object key : configProps.keySet()) {
            String keyStr = (String) key;
            String[] parts = keyStr.split(SEARCH_PROPS_NS);
            if (parts.length > 1) {
                esProps.put(parts[1], configProps.getProperty(keyStr));
            }
        }

        final NodeBuilder builder = nodeBuilder().client(true);

        try {

            File file = new File(esProps.get(SEARCH_PROPS_CFG_PATH));
            if (!file.exists() || !file.isFile()) {
                file = new File(this.getClass().getClassLoader().getResource(esProps.get(SEARCH_PROPS_CFG_PATH)).getFile());
            }
            String configStr = FileUtils.readFileToString(file, "utf-8");
            builder.settings().put(new YamlSettingsLoader().load(configStr));
            builder.settings().put(SEARCH_PROPS_NODE_NAME, builder.settings().get(SEARCH_PROPS_NODE_NAME).toLowerCase() + "-" + java.net.InetAddress.getLocalHost().getHostName().toLowerCase());

        } catch (IOException e) {
            throw new EmbeddedESClientNodeRuntimeException("Search is not initialized properly, the config file could not be found.", e);
        }

        synchronized (this) {
            try {
                node = builder.node();
                client = node.client();
            } catch (ElasticSearchException e) {
                LOG.error("EmbeddedESNodeClient initialization failed : {}", e.getMessage());
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    close();
                } catch (IOException e) {
                    //Ignore
                }
            }
        });

        idxName = esProps.get(SEARCH_PROPS_IDX_NAME) != null ? esProps.get(SEARCH_PROPS_IDX_NAME) : SEARCH_DEFAULT_IDX_NAME;
        LOG.info("Index: {}", idxName);

        typeName = esProps.get(SEARCH_PROPS_TYPE_NAME) != null ? esProps.get(SEARCH_PROPS_TYPE_NAME) : SEARCH_DEFAULT_TYPE_NAME;
        LOG.info("Type: {}", typeName);

        int numShards;
        try {
            numShards = Integer.valueOf(esProps.get(SEARCH_PROPS_SHARDS_NUM));
        } catch (NumberFormatException e) {
            numShards = 40;
        }
        LOG.info("Shards: {}", numShards);

        try {
            requestTimeOutMs = new TimeValue(Long.valueOf(esProps.get(SEARCH_PROPS_REQUEST_TIMEOUTMS)));
        } catch (NumberFormatException e) {
            requestTimeOutMs = new TimeValue(2000);
        }
        LOG.info("Timeout in millis: {}", requestTimeOutMs.getMillis());


        try {
            if (!indexExists(idxName)) {
                LOG.info("Creating the index {} for {}", idxName, typeName);
                createTypedIndex(idxName, typeName, numShards);
            }
        } catch (MasterNotDiscoveredException e) {
            LOG.error("WHILE_INITIAL_SETUP_ERROR [{}]\n if this happens the first " +
                    "time the cluster was provisioned, it will result in a corrupted index.",
                    e.getClass().getSimpleName());
        } catch (ElasticSearchTimeoutException e) {
            LOG.error("WHILE_INITIAL_SETUP_ERROR [{}]\n if this happens the first " +
                    "time the cluster was provisioned, it will result in a corrupted index.",
                    e.getClass().getSimpleName());
        }

        int queueSize;
        try {
            queueSize = Integer.valueOf(esProps.get(SEARCH_PROPS_ASYNC_QUEUE_SIZE));
        } catch (NumberFormatException e) {
            queueSize = 100;
        }
        LOG.info("Queue size: {}", queueSize);

        int threadPoolSize;
        try {
            threadPoolSize = Integer.valueOf(esProps.get(SEARCH_PROPS_ASYNC_POOL_SIZE));
        } catch (NumberFormatException e) {
            threadPoolSize = 1;
        }
        LOG.info("ThreadPool size: {}", threadPoolSize);


        int threadPoolTimeoutMs;
        try {
            threadPoolTimeoutMs = Integer.valueOf(esProps.get(SEARCH_POOL_TIMEOUT_MS));
        } catch (NumberFormatException e) {
            threadPoolTimeoutMs = 2000;
        }
        LOG.info("ThreadPool timeout in millis: {}", threadPoolTimeoutMs);

        setSearchThreadPoolSize(threadPoolSize, queueSize, threadPoolTimeoutMs);

        LOG.info("EmbeddedESClientNode initialization done.");
        initialized = true;
    }


    public boolean indexExists(String indexName) {
        if (client != null) {
            ActionFuture<IndicesExistsResponse> existsFuture = client.admin().indices().exists(new IndicesExistsRequest(indexName));

            return existsFuture.actionGet(requestTimeOutMs).isExists();
        }

        logError("CLIENT-NOT-INITIALIZED", "IDX-EXISTS", indexName, "");
        return false;
	}

    @Override
    public boolean registerRelation(String elementQualifier) {
        try {
            if (isReady()) {

                PutMappingRequestBuilder mappingRequestBuilder = client.admin().indices().preparePutMapping(idxName);
                mappingRequestBuilder.setType(typeName);

                Map<String, Map<String, Map<String, String>>> source = Collections.singletonMap(
                        "properties", Collections.singletonMap(elementQualifier, Collections.singletonMap("type", "nested")));


                mappingRequestBuilder.setSource(Collections.singletonMap("nested_" + elementQualifier, source));
                return client.admin().indices().putMapping(mappingRequestBuilder.request()).actionGet(requestTimeOutMs).isAcknowledged();
            }
        } catch (ElasticSearchException e) {
            logError("REGISTER-" + e.getClass().getName().toUpperCase(), "REG-RELATION", idxName, typeName, elementQualifier);
        }

        logError("CLIENT-NOT-INITIALIZED", "REG-RELATION",idxName, elementQualifier);
        return false;
    }

    public boolean upsert(String indexName, String type, T indexee, boolean fsync) {
        if (isReady()) {


            long t0 = System.currentTimeMillis();

            try {
                IndexRequestBuilder idxBuilder = client.prepareIndex(indexName, type, indexee.getId());
                idxBuilder.setContentType(XContentType.JSON);

                if (fsync) {
                    idxBuilder.setConsistencyLevel(WriteConsistencyLevel.ALL);
                    idxBuilder.setRefresh(fsync);
                }


                if (LOG.isDebugEnabled()) {
                    Map<String, Object> m = indexee.toMap();
                    for (Map.Entry<String, Object> e : m.entrySet()) {
                        LOG.debug("Indexee {} : {}", e.getKey(), e.getValue());
                    }
                }

                try {
                    XContentBuilder contentBuilder = jsonBuilder().map(indexee.toMap());
                    idxBuilder.setSource(contentBuilder);
                    idxBuilder.setTimeout(requestTimeOutMs);
                    client.index(idxBuilder.request()).actionGet();
                    return true;
                } catch (ElasticSearchException e) {
                    LOG.error(e.getMessage(), e);
                    logError(e.getMessage(), "UPSERT", indexName, type, indexee.getId());
                    return false;
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                    logError(e.getMessage(), "UPSERT", indexName, type, indexee.getId());
                    return false;
                }
            } finally {
               LOG.info("ES-UPSERT took {} ms", System.currentTimeMillis() - t0);
            }
        } else {
            logError("CLIENT-NOT-INITIALIZED", "UPSERT",indexName, type, indexee.getId());
            return false;
        }
    }

	public boolean remove(String indexName, String type, String _id, boolean fsync) {
		if (isReady()) {

            long t0 = System.currentTimeMillis();

            try {
                if (null == _id) {
                    throw new EmbeddedESClientNodeRuntimeException("Id cannot be null.");
                }

                DeleteRequestBuilder builder = client.prepareDelete(indexName, type, _id);
                if (fsync) {
                    builder.setConsistencyLevel(WriteConsistencyLevel.QUORUM);
                    builder.setRefresh(fsync);
                }

                builder.setTimeout(requestTimeOutMs);

                try {
                    client.delete(builder.request()).actionGet();
                } catch (ElasticSearchException e) {
                    LOG.error(e.getMessage(), e);
                    logError(e.getMessage(), "REMOVE", indexName, type, _id);
                    return false;
                }
            } finally {
                LOG.info("ES-REMOVE took {} ms", System.currentTimeMillis() - t0);
            }
        } else {
            logError("CLIENT-NOT-INITIALIZED", "REMOVE", indexName, type, _id);
            return false;
        }

        return true;
	}

	public long countEqual(String indexName, String type, Term term) {
        if (isReady()) {

            long t0 = System.currentTimeMillis();
            try {
                if (null == term || null == term.getName() || null == term.getValue() || null == indexName || null == type) {
                    throw new EmbeddedESClientNodeRuntimeException("queryTerm, queryValue, indexName or type cannot be null.");
                }

                CountRequestBuilder countBuilder = client.prepareCount(indexName);
                countBuilder.setTypes(type);

                QueryBuilder queryBuilder;
                if (term.isRelation()) {
                    queryBuilder = QueryBuilders.nestedQuery(term.getPath(), QueryBuilders.termQuery(term.getName(), term.getValue().toLowerCase()));
                } else {
                    queryBuilder = QueryBuilders.termQuery(term.getName(), term.getValue().toLowerCase());
                }

                countBuilder.setQuery(queryBuilder);

                LOG.debug("Count Query {}" , countBuilder.request().toString());

                try {
                    return client.count(countBuilder.request()).actionGet().getCount();
                } catch (ElasticSearchException e) {
                    LOG.error(e.getMessage(), e);
                    logError(e.getMessage(), "COUNT-EQ", indexName, type,  "{" + term.getName() + ":" + term.getValue() + "}");
                    return -1L;
                }
            } finally {
                LOG.info("ES-COUNT took {} ms", System.currentTimeMillis() - t0);
            }
        }

        logError("CLIENT-NOT-INITIALIZED", "COUNT-EQ", indexName, type,  "{" + term.getName() + ":" + term.getValue() + "}");
        return -2L;
	}

	public boolean createTypedIndex(String idxName, String type, int numShards) {
        if (isReady()) {
            CreateIndexRequest createRequest = new CreateIndexRequest(idxName);
            createRequest.settings(getDefaultIndexAnalysis());

            try {
                final ActionFuture<CreateIndexResponse> createFuture = client.admin().indices().create(createRequest);
                final boolean acknowledged = createFuture.actionGet(requestTimeOutMs).isAcknowledged();
                if (!acknowledged) {
                    return false;
                }
            } catch (IndexAlreadyExistsException e) {
                //Ignore
            } catch (ElasticSearchException e) {
                LOG.error(e.getMessage(), e);
                LOG.error("IDX-CREATE-FAILURE [{}:{}]", idxName, e.getMessage());
            }

            PutIndexTemplateRequestBuilder builder = client.admin().indices().preparePutTemplate(idxName);
            Map<String, Object> typeMapping = new HashMap<String, Object>();
            populateTypeSourceWithDefaults(typeMapping, idxName, type, numShards);
            builder.setSource(typeMapping);
            final PutIndexTemplateRequest templateRequest = builder.request();

            try {
                return client.admin().indices().putTemplate(templateRequest).actionGet(requestTimeOutMs).isAcknowledged();
            } catch (ElasticSearchException e) {
                LOG.error(e.getMessage(), e);
                LOG.error("IDX-TEMPLATE-PUT-FAILURE [{}/{}:{}]", new String[] {idxName, type, e.getMessage()});
            }

        }
        return false;
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

	private void populateTypeSourceWithDefaults(final Map<String, Object> typeSrc, final String idxName, final String typeName, int numShards) {
		
		typeSrc.put("template", idxName);
		typeSrc.put("order", 0);

		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("number_of_shards", numShards);
		typeSrc.put("settings", settings);

		Map<String, Object> def = new HashMap<String, Object>();
		Map<String, Object> analyzer = new HashMap<String, Object>();
		Map<String, Object> analysis = new HashMap<String, Object>();
		Map<String, Object> index = new HashMap<String, Object>();
		def.put("type", "keyword");
		analyzer.put("default", def);
		analysis.put("analyzer", analyzer);
		index.put("analysis", analysis);
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
		all.put("enabled", Boolean.FALSE);
		typeMapping.put("_all", all);
		source.put("enabled", Boolean.TRUE);
		source.put("compress", Boolean.TRUE);
		source.put("excludes", new ArrayList<String>() {{ add("__*"); add("_id");} private static final long serialVersionUID = 1L;});
		typeMapping.put("_source", source);
		mapping.put("type", "string");
		mapping.put("index", "analyzed");

        baseMappingTemplate.put("match", "*");
		baseMappingTemplate.put("match_mapping_type", "string");
		baseMappingTemplate.put("mapping", mapping);
		dynTemplatesList.add(new HashMap<String, Object>());
		dynTemplatesList.get(0).put("baseMappingTemplate", baseMappingTemplate);
		typeMapping.put("dynamic_templates", dynTemplatesList);
		mappings.put(typeName, typeMapping);
		typeSrc.put("mappings", mappings);
	}

	public void close() throws IOException {

		if (client != null) {
			client.close();
		}

		if (node != null) {
			node.close();
		}

	}

	@Override
	public boolean upsert(T indexee) {
		return upsert(idxName, typeName, indexee, false);
	}

	@Override
	public boolean remove(String _id) {
		return remove(idxName, typeName, _id, false);
		
	}
	
	public void upsert(T indexee, boolean fsync) {
		upsert(idxName, typeName, indexee, fsync);
	}

	public void remove(String _id, boolean fsync) {
		remove(idxName, typeName, _id, fsync);
		
	}

	@Override
	public long countEqual(Term term) {
		return countEqual(idxName, typeName, term);
	}
	
	private void logError(String operation, String indexName, String type, String _id) {
		LOG.error("SEARCH-FAILURE:{}:{}/{}/{}", new String[] {operation, indexName, type, _id});
	}

	private void logError(String error, String operation, String indexName, String type, String _id) {
		logError(error.toUpperCase() + ":" + operation, indexName, type, _id);
	}

    public List<T> lookup(List<Term> terms, Integer limit, Map<String, ?> constrains, Class<T> impl) {
        return lookup(idxName, typeName, terms, limit, constrains, impl);
    }

    @Override
    public void postProcessSetup(PostActivity... dosAfter) {
        //Skip - Deprecated.
    }

    @SuppressWarnings("unchecked")
    private FilterBuilder buildFilterClauses(Map<String, ?> constrains) {

        if (null != constrains && !constrains.isEmpty()) {
            AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter();
            if (constrains.containsKey("exists")) {

                List<Term> existsTerms = (List<Term>) constrains.get("exists");

                for (Term filterTerm : existsTerms) {
                    andFilterBuilder.add(filterTerm.isRelation()
                            ? (nestedFilter(filterTerm.getPath(), existsFilter(filterTerm.getName())))
                            : andFilterBuilder.add(existsFilter(filterTerm.getName())));
                }
            }
            return andFilterBuilder;
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<T> lookup(String idxName, String typeName, List<Term> terms, Integer limit, Map<String, ?> constrains, Class<T> impl) {
        if (isReady()) {

            long t0 = System.currentTimeMillis();

            try {
                if (null == terms || terms.isEmpty() || null == idxName || null == typeName) {
                    throw new EmbeddedESClientNodeRuntimeException("queryTerm, indexName or type cannot be null.");
                }

                SearchRequestBuilder requestBuilder = client.prepareSearch(idxName);
                requestBuilder.setTypes(typeName);

                requestBuilder.setFrom(0);
                requestBuilder.setSize(Math.min(limit == null || limit <= 0 ? LIMIT_DEFAULT : limit, LIMIT_MAX));

                FilterBuilder filters = buildFilterClauses(constrains);
                if (filters != null) {
                    requestBuilder.setFilter(filters);
                }

                BoolQueryBuilder boolQueryBuilder = boolQuery();
                for (Term term : terms) {

                    QueryBuilder innerQuery;
                    if (term.isRelation()) {
                        innerQuery = nestedQuery(term.getPath(), wildcardQuery(term.getName(), term.getValue().toLowerCase()));
                    } else {
                        innerQuery = wildcardQuery(term.getName(), term.getValue().toLowerCase());
                    }

                    if (term.getOperator() == SearchOperator.AND) {
                        boolQueryBuilder.must(innerQuery);
                    } else if (term.getOperator() == SearchOperator.OR) {
                        boolQueryBuilder.should(innerQuery);
                        boolQueryBuilder.minimumNumberShouldMatch(1);
                    }
                }

                requestBuilder.setQuery(boolQueryBuilder);
                LOG.debug(requestBuilder.toString());

                try {

                    SearchResponse response = requestBuilder.execute().actionGet();

                    List<T> results = new ArrayList<T>();
                    for (SearchHit hit : response.getHits()) {

                        //ES limits are per shard. We will specify a cutover here.
                        if (results.size() >= limit) {

                            LOG.debug("limit hit, breaking...");
                            break;
                        }

                        Indexable oneResult = impl.newInstance();
                        oneResult.fromMap(hit.getId(), hit.sourceAsMap());

                        results.add((T) oneResult);
                    }
                    return results;

                } catch (ElasticSearchException e) {
                    logError(e.getMessage(), "LOOKUP", idxName, typeName,
                            "[{ " + terms.get(0).getName() + " : " + terms.get(0).getValue() + " }, .., { "
                                    + terms.get(terms.size() - 1).getName() + " : { "
                                    + terms.get(terms.size() - 1).getName() + " }]");
                } catch (IllegalAccessException e) {
                    throw new EmbeddedESClientNodeRuntimeException("Error creating instance of " + impl.getSimpleName() + ": " + e.getMessage(), e);
                } catch (InstantiationException e) {
                    throw new EmbeddedESClientNodeRuntimeException("Error creating instance of " + impl.getSimpleName() + ": " + e.getMessage(), e);
                }
            } finally {

                LOG.info("ES-LOOKUP took {} ms", System.currentTimeMillis() - t0);
            }
        }
        logError("CLIENT-NOT-INITIALIZED", "LOOKUP", idxName, typeName,
                "[{ " + terms.get(0).getName() + " : " + terms.get(0).getValue() + " }, .., { "
                        + terms.get(terms.size() - 1).getName() + " : { "
                        + terms.get(terms.size() - 1).getName() + " }]");

        return null;

    }
	
	public void asyncUpsert(final T indexee) {
        try {
            searchThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    upsert(indexee);
                }
            });
        } catch (RejectedExecutionException e) {
            logError(e.getMessage(), "UPSERT", this.idxName, this.typeName, indexee.getId());
        }

	}
	
	public void asyncRemove(final String id) {
        try {
            searchThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    remove(id);
                }
            });
        } catch (RejectedExecutionException e) {
            logError(e.getMessage(), "REMOVE", this.idxName, this.typeName, id);
        }
    }



    public void setSearchThreadPoolSize(int searchThreadPoolSize, int queueSize, int timeout) {

        this.searchThreadPool = new ThreadPoolExecutor(searchThreadPoolSize, searchThreadPoolSize,
                timeout, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(queueSize));
    }



}
