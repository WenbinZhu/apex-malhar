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

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.datatorrent.netlet.util.DTThrowable;

public abstract class AbstractElasticDateIndexedOutputOperator<T extends Map<String, Object>> extends AbstractElasticOutputOperator<T>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractElasticDateIndexedOutputOperator.class);

  private String dateField;
  private String typeField;
  private String idField;

  private ElasticConfiguration config;
  private Set<String> createdIndexes;

  protected AbstractElasticDateIndexedOutputOperator(ElasticConfiguration config)
  {
    this.config = config;
    this.createdIndexes = new HashSet<>();
    this.store = new ElasticStore(config.getClusterName(), config.getHosts());
    this.batchSize = config.getBatchSize();
    this.bulkSizeMB = config.getBulkSizeMB();
    this.flushIntervalMS = config.getFlushIntervalMS();
    this.bulkRetryTimeMS = config.getBulkRetryTimeMS();
    this.bulkRetryCount = config.getBulkRetryCount();
  }

  @Override
  protected String getIndexByTuple(T tuple)
  {
    if (dateField == null || !tuple.containsKey(dateField)) {
      logger.error("Date field not found in tuple: {}", tuple.toString());
      DTThrowable.rethrow(new Exception("Date field not found in tuple: " + tuple.toString()));
    } else {
      Date date = (Date)tuple.get(dateField);
      DateTimeFormatter fmt = DateTimeFormat.forPattern(config.getIndexPattern());
      String indexSuffix = fmt.print(new DateTime(date));
      String tupleIndex = config.getIndexPrefix() + indexSuffix;

      if (!createdIndexes.contains(tupleIndex)) {
        // Create index in ElasticSearch
        IndicesAdminClient indicesAdminClient = store.client.admin().indices();
        IndicesExistsResponse res = indicesAdminClient.prepareExists(tupleIndex).get();
        if (!res.isExists()) {
          indicesAdminClient.prepareCreate(tupleIndex)
            .setSettings(Settings.builder()
              .put("index.number_of_shards", config.getIndexShards())
              .put("index.number_of_replicas", config.getIndexReplicas())
            )
            .get();

          createTypeMappings(indicesAdminClient, tupleIndex);
        }
      }
      return tupleIndex;
    }

    return null;
  }

  @Override
  protected String getTypeByTuple(T tuple)
  {
    if (typeField == null || !tuple.containsKey(typeField)) {
      logger.error("Type field not found in tuple: {}", tuple.toString());
      DTThrowable.rethrow(new Exception("Type field not found in tuple: " + tuple.toString()));
    } else {
      String tupleType = (String)tuple.get(typeField);
      if (!config.getTypeMappings().keySet().contains(tupleType)) {
        logger.error("Type not declared in configuration for tuple: {}", tuple.toString());
      } else {
        return tupleType;
      }
    }

    return null;
  }

  @Override
  protected String getIdByTuple(T tuple)
  {
    return (idField == null || tuple.containsKey(idField)) ? null : (String)tuple.get(idField);
  }

  @Override
  protected abstract IndexRequest setSource(IndexRequest indexRequest, T tuple);

  private void createTypeMappings(IndicesAdminClient indicesAdminClient, String index)
  {
    for (Map.Entry<String, String> mapping : config.getTypeMappings().entrySet()) {
      indicesAdminClient.preparePutMapping(index)
        .setType(mapping.getKey())
        .setSource(mapping.getValue(), XContentType.JSON)
        .get();
    }
  }

  public Set<String> getCreatedIndexes()
  {
    return Collections.unmodifiableSet(createdIndexes);
  }

  public ElasticConfiguration getConfig()
  {
    return config;
  }

  public void setElasticConfiguration(ElasticConfiguration config)
  {
    this.config = config;
  }

  public String getDateField()
  {
    return dateField;
  }

  public void setDateField(String dateField)
  {
    this.dateField = dateField;
  }

  public String getTypeField()
  {
    return typeField;
  }

  public void setTypeField(String typeField)
  {
    this.typeField = typeField;
  }

  public String getIdField()
  {
    return idField;
  }

  public void setIdField(String idField)
  {
    this.idField = idField;
  }
}
