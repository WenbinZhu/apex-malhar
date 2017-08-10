package org.apache.apex.malhar.elastic;

import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;

/**
 * Concrete class implementation of ElasticSearch date indexed output operator.
 *
 * @param <T> tuple in map format
 */
public class ElasticDateIndexOutputOperator<T extends Map<String, Object>> extends AbstractElasticDateIndexedOutputOperator<T>
{
  ElasticDateIndexOutputOperator(ElasticConfiguration config)
  {
    super(config);
  }

  @Override
  protected IndexRequest setSource(IndexRequest indexRequest, T tuple)
  {
    return indexRequest.source(tuple);
  }
}
