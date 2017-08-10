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

import javax.validation.constraints.Min;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.lib.db.AbstractStoreOutputOperator;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Basic implementation of an output operator for ElasticSearch. <br>
 * This class will create a bulk processor for processing tyuples in batch.
 *
 * @param <T> generic tuple
 */
public abstract class AbstractElasticOutputOperator<T> extends AbstractStoreOutputOperator<T, ElasticStore>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractElasticOutputOperator.class);

  @Min(1)
  protected int batchSize = 1000;
  @Min(1)
  protected int bulkSizeMB = 5;
  @Min(1)
  protected int flushIntervalMS = 5000;
  @Min(1)
  protected int bulkRetryTimeMS = 100;
  @Min(0)
  protected int bulkRetryCount = 3;

  protected transient BulkProcessor bulkProcessor;

  protected AbstractElasticOutputOperator()
  {
  }

  protected AbstractElasticOutputOperator(String clusterName, String hostAddrs)
  {
    this.store = new ElasticStore(clusterName, hostAddrs);
  }

  /**
   * Create a transient bulk processor.
   *
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    createBulkProcessor();
  }

  /**
   * close the bulk processor.
   *
   */
  @Override
  public void teardown()
  {
    super.teardown();
    bulkProcessor.close();
  }

  /**
   * Flush the batch to ElasticSearch at the end of each window.
   *
   */
  @Override
  public void endWindow()
  {
    super.endWindow();
    bulkProcessor.flush();
  }

  /**
   * Add tuple to the bulk processor. <br>
   *
   * The bulk processor will flush the batch automatically if <br>
   * 1) The number of tuples in the batch reaches <code>batchSize</code> or <br>
   * 2) <code>flushIntervalMS</code> has passed since last flush or <br>
   * 3) The size of the batch reaches <code>bulkSizeMB</code>
   */
  @Override
  public void processTuple(T tuple)
  {
    IndexRequest indexRequest = new IndexRequest();
    String tupleIndex = getIndexByTuple(tuple);
    String tupleType = getTypeByTuple(tuple);
    String tupleId = getIdByTuple(tuple);
    if (tupleIndex == null || tupleType == null) {
      return;
    }

    indexRequest.index(tupleIndex);
    indexRequest.type(tupleType);
    if (tupleId != null) {
      indexRequest.id(tupleId);
    }

    bulkProcessor.add(setSource(indexRequest, tuple));
  }

  private void createBulkProcessor()
  {
    bulkProcessor = BulkProcessor.builder(store.client, new BulkProcessor.Listener()
    {
      @Override
      public void beforeBulk(long executionId, BulkRequest bulkRequest)
      {
        logger.debug("Prepare to send {} tuples in a bulk with executionId: {}", bulkRequest.numberOfActions(), executionId);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse)
      {
        if (bulkResponse.hasFailures()) {
          logger.error("Failures occur in bulk request with executionId: {}", executionId);
          DTThrowable.rethrow(new Exception(bulkResponse.buildFailureMessage()));
        }
      }

      @Override
      public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable)
      {
        logger.error("Failed to process bulk request with executionId: {}", executionId);
        DTThrowable.rethrow(new Exception(throwable));
      }
    })
      .setBulkActions(batchSize)
      .setBulkSize(new ByteSizeValue(bulkSizeMB, ByteSizeUnit.MB))
      .setFlushInterval(TimeValue.timeValueMillis(flushIntervalMS))
      .setConcurrentRequests(1)
      .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(bulkRetryTimeMS), bulkRetryCount))
      .build();
  }

  /**
   * User defined function for setting the index of the tuple to store to.
   *
   * @param tuple generic tuple
   */
  protected abstract String getIndexByTuple(T tuple);

  /**
   * User defined function for setting the type of the tuple to store to.
   *
   * @param tuple generic tuple
   */
  protected abstract String getTypeByTuple(T tuple);

  /**
   * User defined function for setting the index of the tuple. <br>
   *
   * This function can return null to indicate that there is no unique id for the tuple.
   * In this case, ElasticSearch will add an auto-generated _id field to the document.
   *
   * @param tuple generic tuple
   */
  protected abstract String getIdByTuple(T tuple);

  /**
   * User defined function to format the tuple to one of the source types supported by IndexRequest.
   *
   * @param indexRequest ElasticSearch index request object
   * @param tuple generic tuple
   */
  protected abstract IndexRequest setSource(IndexRequest indexRequest, T tuple);

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public void setBulkSizeMB(int bulkSizeMB)
  {
    this.bulkSizeMB = bulkSizeMB;
  }

  public void setFlushIntervalMS(int flushIntervalMS)
  {
    this.flushIntervalMS = flushIntervalMS;
  }

  public void setBulkRetryTimeMS(int bulkRetryTimeMS)
  {
    this.bulkRetryTimeMS = bulkRetryTimeMS;
  }

  public void setBulkRetryCount(int bulkRetryCount)
  {
    this.bulkRetryCount = bulkRetryCount;
  }
}
