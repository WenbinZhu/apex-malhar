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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.validation.constraints.NotNull;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import com.datatorrent.lib.db.Connectable;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * ElasticSearch base connector which connects to an ElasticSearch cluster.
 *
 */
public class ElasticStore implements Connectable
{
  private static final Logger logger = LoggerFactory.getLogger(ElasticStore.class);

  private static final int DEFAULT_PORT = 9300;

  protected String clusterName;
  @NotNull
  protected String hostAddrs;

  protected TransportClient client;

  /**
   * Constructor for setting cluster name and host addresses.
   *
   * @param clusterName name of the cluster
   * @param hostAddrs nodes addresses strings separated by comma,
   *                  each address should be host:port
   */
  public ElasticStore(String clusterName, String hostAddrs)
  {
    this.clusterName = clusterName;
    this.hostAddrs = hostAddrs;
  }

  /* (non-Javadoc)
   *
   * @see com.datatorrent.lib.db.Connectable#connect()
   */
  @Override
  public void connect() throws IOException
  {
    Settings settings;
    Settings.Builder settingsBuilder = Settings.builder();

    if (StringUtils.isEmpty(clusterName)) {
      settings = settingsBuilder.build();
    } else {
      settings = settingsBuilder.put("cluster.name", clusterName).build();
    }
    client = new PreBuiltTransportClient(settings);

    String[] hosts = hostAddrs.split(",");
    for (String host : hosts) {
      String[] addrPair = host.split(":");
      String hostName = addrPair[0].trim();
      int port = NumberUtils.toInt(addrPair[1].trim(), DEFAULT_PORT);

      try {
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));
      } catch (UnknownHostException ex) {
        logger.error("Unknown node host for cluster {}", clusterName == null ? "elasticsearch" : clusterName, ex);
        DTThrowable.rethrow(ex);
      }
    }
  }

  /* (non-Javadoc)
   *
   * @see com.datatorrent.lib.db.Connectable#disconnect()
   */
  @Override
  public void disconnect() throws IOException
  {
    if (client != null) {
      client.close();
    }
  }

  /* (non-Javadoc)
   *
   * @see com.datatorrent.lib.db.Connectable#isConnected()
   */
  @Override
  public boolean isConnected()
  {
    return client != null && client.connectedNodes().size() != 0;
  }
}
