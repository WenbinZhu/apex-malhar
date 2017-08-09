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

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ElasticOutputOperatorTest
{
  private String configString = "{\n" +
    "    \"cluster_name\": \"dt-cluster-0\",\n" +
    "    \"hosts\":  \"localhost:9300\",\n" +
    "    \"batch_size\": 5,\n" +
    "    \"flush_interval_ms\": 1000,\n" +
    "    \"type_mappings\": {\n" +
    "        \"user\": {\n" +
    "            \"properties\": {\n" +
    "                \"name\": {\n" +
    "                    \"type\": \"text\"\n" +
    "                },\n" +
    "                \"salary\": {\n" +
    "                    \"type\": \"long\"\n" +
    "                }\n" +
    "            }\n" +
    "        }\n" +
    "    },\n" +
    "    \"date_field\": \"date\",\n" +
    "    \"type_field\": \"type\",\n" +
    "    \"id_field\": \"id\"\n" +
    "}";

  @Test
  public void testConfiguration()
  {
    ElasticConfiguration config = new ElasticConfiguration(configString);
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
    ElasticConfiguration config = new ElasticConfiguration(configString);
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
        tuple.put("salary", (int) (Math.random() * 1000000));
        operator.processTuple(tuple);
      }
      operator.endWindow();
    }
    operator.teardown();
  }
}
