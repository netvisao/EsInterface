package com.a.search;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.aol.identity.concurrency.ConcurrentExecutor;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchInterruptedException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.loader.YamlSettingsLoader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedESClientNode<T extends Indexable> implements Closeable, Search<T> {

	private static final String SRCH_DEFAULT_IDX_NAME = "index";
	private static final String SRCH_DEFAULT_TYPE_NAME = "gentype";
	private static final String SRCH_PROPS_NS = "com.aol.search.es.";
	private static final String SRCH_PROPS_CFG_PATH = "path.conf";
	private static final String SRCH_PROPS_IDX_NAME = "index.name";
	private static final String SRCH_PROPS_TYPE_NAME = "type.name";
    private static final String SRCH_PROPS_NODE_NAME = "node.name";
	private static final String SRCH_PROPS_SHARDS_NUM = "shards.num";
    private static final String SRCH_PROPS_ASYNC_POOL_SIZE = "async.thread.pool";
    private static final String SRCH_PROPS_ASYNC_QUEUE_SIZE = "async.queue.size";
    public final static int SEARCH_POOL_TIMEOUT_MS = 2000;

    private ThreadPoolExecutor searchThreadPool;

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedESClientNode.class);
    private final String typeName;
	private final String idxName;
	private Client client = null;
	private Node node = null;

	private EmbeddedESClientNode(Properties configProps) {
		LOG.info("EmbeddedESClientNode initializing.");
		
		if (null == configProps) {
			throw new RuntimeException("Search is not initialized properly, provided properties are null.");
		}

        Map<String, String> esProps = new HashMap<String, String>();
        for (Object key : configProps.keySet()) {
			String keyStr = (String) key;
			String[] parts = keyStr.split(SRCH_PROPS_NS);
			if (parts.length > 1) {
				esProps.put(parts[1], configProps.getProperty(keyStr));
			}
		}

		final NodeBuilder builder = nodeBuilder().client(true);

		try {

			File file = new File(esProps.get(SRCH_PROPS_CFG_PATH));
			if (!file.exists() || !file.isFile()) {
				file = new File(this.getClass().getClassLoader().getResource(esProps.get(SRCH_PROPS_CFG_PATH)).getFile());
			}
			String configStr = FileUtils.readFileToString(file, "utf-8");
			builder.settings().put(new YamlSettingsLoader().load(configStr));
            builder.settings().put(SRCH_PROPS_NODE_NAME, builder.settings().get(SRCH_PROPS_NODE_NAME).toLowerCase() + "-" + java.net.InetAddress.getLocalHost().getHostName().toLowerCase());

		} catch (IOException e) {
			throw new RuntimeException("Search is not initialized properly, the config file could not be found.");
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

		idxName = esProps.get(SRCH_PROPS_IDX_NAME) != null ? esProps.get(SRCH_PROPS_IDX_NAME) : SRCH_DEFAULT_IDX_NAME;
		typeName = esProps.get(SRCH_PROPS_TYPE_NAME) != null ? esProps.get(SRCH_PROPS_TYPE_NAME) : SRCH_DEFAULT_TYPE_NAME;
		
		int num_shards;
		try {
			num_shards = Integer.valueOf(esProps.get(SRCH_PROPS_SHARDS_NUM));
		} catch (NumberFormatException e) {
			num_shards = 40;
		} catch (NullPointerException e) {
            num_shards = 40;
        }
		
		createTypedIndex(idxName, typeName, num_shards);

        int queueSize;
        try {
            queueSize = Integer.valueOf(esProps.get(SRCH_PROPS_ASYNC_QUEUE_SIZE));
        } catch (NumberFormatException e) {
            queueSize = 100;
        }

        int threadPoolSize;
        try {
            threadPoolSize = Integer.valueOf(esProps.get(SRCH_PROPS_ASYNC_POOL_SIZE));
        } catch (NumberFormatException e) {
            threadPoolSize = 1;
        }

        setSearchThreadPoolSize(threadPoolSize, queueSize);

		LOG.info("EmbeddedESClientNode initialization done.");
	}

	public boolean indexExists(String indexName) {
        if (client != null) {
            ActionFuture<IndicesExistsResponse> existsFuture = client.admin().indices().exists(new IndicesExistsRequest(indexName));
            return existsFuture.actionGet().exists();
        }

        logError("CLIENT-NOT-INITIALIZED", "IDX-EXISTS",indexName, indexName);
        return false;
	}

	public void upsert(String indexName, String type, T indexee, boolean fsync) {
        if (client != null) {
            IndexRequestBuilder idxBuilder = client.prepareIndex(indexName, type, indexee.getId());
            idxBuilder.setContentType(XContentType.JSON);

            if (fsync) {
                idxBuilder.setConsistencyLevel(WriteConsistencyLevel.ALL);
                idxBuilder.setRefresh(fsync);
            }

            try {
                XContentBuilder contentBuilder = jsonBuilder().map(indexee.toMap());
                idxBuilder.setSource(contentBuilder);
                client.index(idxBuilder.request()).actionGet();
            } catch (ElasticSearchException e) {
                logError(e.getMessage(), "UPSERT", indexName, type, indexee.getId());
            } catch (IOException e) {
                logError(e.getMessage(), "UPSERT", indexName, type, indexee.getId());
            }
        } else {
            logError("CLIENT-NOT-INITIALIZED", "UPSERT",indexName, type, indexee.getId());
        }
    }

	public void remove(String indexName, String type, String _id, boolean fsync) {
		if (client != null) {
            if (null == _id) {
                throw new NullPointerException("Id cannot be null.");
            }

            DeleteRequestBuilder builder = client.prepareDelete(indexName, type, _id);
            if (fsync) {
                builder.setConsistencyLevel(WriteConsistencyLevel.ALL);
                builder.setRefresh(fsync);
            }
            try {
                client.delete(builder.request()).actionGet();
            } catch (ElasticSearchException e) {
                logError(e.getMessage(), "REMOVE", indexName, type, _id);
            }
        } else {
            logError("CLIENT-NOT-INITIALIZED", "REMOVE", indexName, type, _id);
        }
	}

	public long countEqual(String indexName, String type, String queryTerm, String queryValue) {
        if (client != null) {
            if (null == queryTerm || null == queryValue || null == indexName || null == type) {
                throw new NullPointerException("queryTerm, queryValue, indexName or type cannot be null.");
            }

            CountRequestBuilder countBuilder = client.prepareCount(indexName);
            countBuilder.setTypes(type);
            QueryBuilder queryBuilder = QueryBuilders.termQuery(queryTerm, queryValue);

            countBuilder.setQuery(queryBuilder);
            try {
                return client.count(countBuilder.request()).actionGet().count();
            } catch (ElasticSearchInterruptedException e) {
                logError(e.getMessage(), "COUNT-EQ", indexName, type,  "{" + queryTerm + ":" + queryValue + "}");
                return -1L;
            }
        }
        logError("CLIENT-NOT-INITIALIZED", "COUNT-EQ", indexName, type,  "{" + queryTerm + ":" + queryValue + "}");
        return -2L;
	}

	public boolean createTypedIndex(String idxName, String type, int numShards) {
        if (client != null) {
            try {
                final ActionFuture<CreateIndexResponse> createFuture = client.admin().indices().create(new CreateIndexRequest(idxName));
                final boolean acknowledged = createFuture.actionGet().acknowledged();
                if (!acknowledged) {
                    return false;
                }
            } catch (IndexAlreadyExistsException e) {
                //Ignore
            } catch (ElasticSearchException e) {
                LOG.error("IDX-CREATE-FAILURE [{}:{}]", idxName, e.getMessage());
            }

            PutIndexTemplateRequestBuilder builder = client.admin().indices().preparePutTemplate(idxName);
            Map<String, Object> typeMapping = new HashMap<String, Object>();
            populateTypeSourceWithDefaults(typeMapping, idxName, type, numShards);
            builder.setSource(typeMapping);
            final PutIndexTemplateRequest templateRequest = builder.request();

            try {
                return client.admin().indices().putTemplate(templateRequest).actionGet().acknowledged();
            } catch (ElasticSearchException e) {
                LOG.error("IDX-TEMPLATE-PUT-FAILURE [{}/{}:{}]", new String[] {idxName, type, e.getMessage()});
            }

        }
        return false;
    }

	public void populateTypeSourceWithDefaults(final Map<String, Object> typeSrc, final String idxName, final String typeName, int numShards) {
		
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
		mapping.put("index", "not_analyzed");
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
	public void upsert(T indexee) {
		upsert(idxName, typeName, indexee, false);
	}

	@Override
	public void remove(String _id) {
		remove(idxName, typeName, _id, false);
		
	}
	
	public void upsert(T indexee, boolean fsync) {
		upsert(idxName, typeName, indexee, fsync);
	}

	public void remove(String _id, boolean fsync) {
		remove(idxName, typeName, _id, fsync);
		
	}

	@Override
	public long countEqual(String queryTerm, String queryValue) {
		return countEqual(idxName, typeName, queryTerm, queryValue);
	}
	
	private void logError(String operation, String indexName, String type, String _id) {
		LOG.error("SRCH-FAILURE:{}:{}/{}/{}", new String[] {operation, indexName, type, _id});
	}
	private void logError(String error, String operation, String indexName, String type, String _id) {
		logError(error.toUpperCase() + ":" + operation, indexName, type, _id);
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



    public void setSearchThreadPoolSize(int searchThreadPoolSize, int queueSize) {

        this.searchThreadPool = new ThreadPoolExecutor(searchThreadPoolSize, searchThreadPoolSize,
                SEARCH_POOL_TIMEOUT_MS, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(queueSize));

        LOG.info("SearchThreadPool setup with PoolSize:{} QueueSize:{} TimeOutMS:{}",new Object[] {searchThreadPoolSize, queueSize, SEARCH_POOL_TIMEOUT_MS});

    }

}
