/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.malhar.elastic;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.DTThrowable;

public class ElasticConfiguration
{
  private static final Logger logger = LoggerFactory.getLogger(ElasticConfiguration.class);

  private static final String CLUSTER_NAME_KEY = "cluster_name";
  private static final String HOSTS_KEY = "hosts";
  private static final String INDEX_PREFIX_KEY = "index_prefix";
  private static final String INDEX_PATTERN_KEY = "index_pattern";
  private static final String INDEX_SHARDS_KEY = "index_shards";
  private static final String INDEX_REPLICAS_KEY = "index_replicas";
  private static final String TYPE_MAPPINGS_KEY = "type_mappings";
  private static final String BATCH_SIZE_KEY = "batch_size";
  private static final String BULK_SIZE_MB_KEY = "bulk_size_mb";
  private static final String FLUSH_INTERVAL_MS_KEY = "flush_interval_ms";
  private static final String BULK_RETRY_TIME_MS_KEY = "bulk_retry_time_ms";
  private static final String BULK_RETRY_COUNT_kEY = "bulk_retry_count";

  private final String DEFAULT_INDEX_PREFIX = "dt-";
  private final String DEFAULT_INDEX_PATTERN = "MM/dd/yyyy HH:mm:ss";
  private final int DEFAULT_INDEX_SHARDS = 3;
  private final int DEFAULT_INDEX_REPLICAS = 2;
  private final int DEFAULT_BATCH_SIZE = 1000;
  private final int DEFAULT_BULK_SIZE_MB = 5;
  private final int DEFAULT_FLUSH_INTERVAL_MS = 5000;
  private final int DEFAULT_BULK_RETRY_TIME_MS = 100;
  private final int DEFAULT_BULK_RETRY_COUNT = 3;

  private String clusterName;
  private String hosts;
  private String indexPrefix;
  private String indexPattern;
  private int indexShards;
  private int indexReplicas;
  private Map<String, String> typeMappings;
  private int batchSize;
  private int bulkSizeMB;
  private int flushIntervalMS;
  private int bulkRetryTimeMS;
  private int bulkRetryCount;

  public ElasticConfiguration(JSONObject jsonConfig)
  {
    this(jsonConfig.toString());
  }

  public ElasticConfiguration(String jsonString)
  {
    JSONObject jsonConfig = null;
    try {
      jsonConfig = new JSONObject(jsonString);
    } catch (JSONException ex) {
      logger.error("Config string is in wrong json format");
      DTThrowable.rethrow(ex);
    }

    this.clusterName = jsonConfig.optString(CLUSTER_NAME_KEY, "");
    this.hosts = jsonConfig.optString(HOSTS_KEY, "");
    this.indexPrefix = jsonConfig.optString(INDEX_PREFIX_KEY, DEFAULT_INDEX_PREFIX);
    this.indexPattern = jsonConfig.optString(INDEX_PATTERN_KEY, DEFAULT_INDEX_PATTERN);
    this.indexShards = jsonConfig.optInt(INDEX_SHARDS_KEY, DEFAULT_INDEX_SHARDS);
    this.indexReplicas = jsonConfig.optInt(INDEX_REPLICAS_KEY, DEFAULT_INDEX_REPLICAS);
    this.batchSize = jsonConfig.optInt(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);
    this.bulkSizeMB = jsonConfig.optInt(BULK_SIZE_MB_KEY, DEFAULT_BULK_SIZE_MB);
    this.flushIntervalMS = jsonConfig.optInt(FLUSH_INTERVAL_MS_KEY, DEFAULT_FLUSH_INTERVAL_MS);
    this.bulkRetryTimeMS = jsonConfig.optInt(BULK_RETRY_TIME_MS_KEY, DEFAULT_BULK_RETRY_TIME_MS);
    this.bulkRetryCount = jsonConfig.optInt(BULK_RETRY_COUNT_kEY, DEFAULT_BULK_RETRY_COUNT);

    // Parse field type mappings
    JSONObject typeMappingsObj = jsonConfig.optJSONObject(TYPE_MAPPINGS_KEY);
    if (typeMappingsObj != null) {
      Iterator typesIter = typeMappingsObj.keys();
      typeMappings = new HashMap<>();

      while (typesIter.hasNext()) {
        String type = (String)typesIter.next();
        JSONObject mapping = typeMappingsObj.optJSONObject(type);
        typeMappings.put(type, mapping.toString());
      }
    }
  }

  public String getClusterName()
  {
    return clusterName;
  }

  public String getHosts()
  {
    return hosts;
  }

  public String getIndexPrefix()
  {
    return indexPrefix;
  }

  public String getIndexPattern()
  {
    return indexPattern;
  }

  public int getIndexShards()
  {
    return indexShards;
  }

  public int getIndexReplicas()
  {
    return indexReplicas;
  }

  public Map<String, String> getTypeMappings()
  {
    return typeMappings;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public int getBulkSizeMB()
  {
    return bulkSizeMB;
  }

  public int getFlushIntervalMS()
  {
    return flushIntervalMS;
  }

  public int getBulkRetryTimeMS()
  {
    return bulkRetryTimeMS;
  }

  public int getBulkRetryCount()
  {
    return bulkRetryCount;
  }
}
