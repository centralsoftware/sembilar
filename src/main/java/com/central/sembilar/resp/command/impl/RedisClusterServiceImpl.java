/**
 * 
 * Copyright 2014 Central Software

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.central.sembilar.resp.command.impl;

import java.io.IOException;
import java.util.List;

import com.central.sembilar.resp.ProtocolConstant;
import com.central.sembilar.resp.RespException;
import com.central.sembilar.resp.RespSerializer;
import com.central.sembilar.resp.cluster.ClusterNode;
import com.central.sembilar.resp.command.ClusterService;
import com.central.sembilar.resp.connection.ConnectionManager;
import com.central.sembilar.resp.connection.RespClient;
import com.central.sembilar.resp.parser.ClusterNodeParser;
import com.central.sembilar.resp.parser.impl.RedisClusterNodeParser;
import com.central.sembilar.resp.type.BulkString;

public class RedisClusterServiceImpl implements ClusterService {
	
	private ConnectionManager connectionManager;
	
	@Override
	public BulkString clusterInfo() throws IOException, RespException {
		RespSerializer serializer = new RespSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_CLUSTER_INFO);
		RespClient client = connectionManager.getClientFromPool();
		BulkString resp = client.send(cmd, BulkString.class);
		return resp;
	}

	@Override
	public BulkString clusterNodes() throws IOException, RespException {
		RespSerializer serializer = new RespSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_CLUSTER_NODES);
		RespClient client = connectionManager.getSeedEndpointClients().get(0);
		BulkString resp = client.send(cmd, BulkString.class);
		return resp;
	}

	@Override
	public void setConnectionManager(
			ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;		
	}

	@Override
	public List<ClusterNode> buildClusterNode() throws IOException, RespException {
		BulkString raw = clusterNodes();
		ClusterNodeParser parser = new RedisClusterNodeParser();
		List<ClusterNode> nodes = parser.parse(raw.getString());
		return nodes;
	}

}
