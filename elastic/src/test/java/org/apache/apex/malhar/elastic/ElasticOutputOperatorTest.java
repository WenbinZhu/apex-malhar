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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.lang.RandomStringUtils;

import com.cedarsoftware.util.io.JsonWriter;

public class ElasticOutputOperatorTest
{
  private JSONObject makeJsonConfig()
  {
    JSONObject config = new JSONObject();
    try {
      config.put("cluster_name", "dt-cluster-0");
      config.put("hosts", "localhost:9300");
      config.put("batch_size", 5);
      config.put("flush_interval_ms", 1000);
      config.put("date_field", "date");
      config.put("type_field", "type");
      config.put("id_field", "id");
      config.put("type_mappings", new JSONObject());
      config.getJSONObject("type_mappings").put("user", new JSONObject());
      config.getJSONObject("type_mappings").getJSONObject("user").put("properties", new JSONObject());
      config.getJSONObject("type_mappings").getJSONObject("user").getJSONObject("properties").put("name", new JSONObject());
      config.getJSONObject("type_mappings").getJSONObject("user").getJSONObject("properties").getJSONObject("name").put("type", "text");
      config.getJSONObject("type_mappings").getJSONObject("user").getJSONObject("properties").put("salary", new JSONObject());
      config.getJSONObject("type_mappings").getJSONObject("user").getJSONObject("properties").getJSONObject("salary").put("type", "long");
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }

    return config;
  }

  private String getJsonConfigString(JSONObject jsonConfig)
  {
    return JsonWriter.formatJson(jsonConfig.toString());
  }

  @Test
  public void testConfiguration()
  {
    JSONObject jsonConfig = makeJsonConfig();
    ElasticConfiguration config = new ElasticConfiguration(jsonConfig);
    Map<String, Map<String, String>> m = new TreeMap<>();
    m.put("company", new TreeMap<String, String>());
    m.get("company").put("name", "text");
    m.get("company").put("profit", "double");
    config.setTypeMappings(m);
    String expect = "{\"properties\":{\"name\":\"text\",\"profit\":\"double\"}}";
    Assert.assertEquals("Incorrect configuration",  expect, config.getTypeMappings().get("company"));
  }

  @Test
  public void testDateIndexedOutput()
  {
    JSONObject jsonConfig = makeJsonConfig();
    ElasticConfiguration config = new ElasticConfiguration(jsonConfig);
    ElasticDateIndexOutputOperator operator = new ElasticDateIndexOutputOperator(config);
    // operator.setDateField("date");
    // operator.setTypeField("type");
    // operator.setIdField("id");

    operator.setup(null);
    for (int windowId = 1; windowId <= 5; windowId++) {
      operator.beginWindow(windowId);
      for (int i = 0; i < 12; i++) {
        Map<String, Object> tuple = new HashMap<String, Object>();
        tuple.put("date", new Date());
        tuple.put("type", "user");
        tuple.put("id", windowId + "." + i);
        tuple.put("name", RandomStringUtils.randomAlphanumeric(5));
        tuple.put("salary", (int)(Math.random() * 1000000));
        operator.processTuple(tuple);
      }
      operator.endWindow();
    }
    operator.teardown();
  }
}
