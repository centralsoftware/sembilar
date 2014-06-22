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

package com.central.sembilar.resp.connection.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.central.varth.resp.RespException;
import com.central.varth.resp.cluster.ClusterNode;
import com.central.varth.resp.connection.RespClient;
import com.central.varth.resp.connection.RespClientFactory;

public class RedisRespClientFactory implements RespClientFactory 
{
	@Override
	public RespClient getInstanceFromAddress(InetSocketAddress address) throws IOException {
		RespClient client = new RedisRespClientImpl(address.getHostString(), address.getPort());
		return client;
	}

	@Override
	public RespClient getInstanceFromNode(ClusterNode node) throws IOException {
		RespClient client = new RedisRespClientImpl(node);		
		return client;
	}

	@Override
	public List<RespClient> getInstancesFromAddress(
			List<InetSocketAddress> addresses) throws IOException {
		List<RespClient> clients = new ArrayList<RespClient>();
		for (InetSocketAddress address:addresses)
		{
			RespClient client = this.getInstanceFromAddress(address);
			clients.add(client);
		}
		return clients;
	}

	@Override
	public List<RespClient> getInstancesFromNodes(List<ClusterNode> nodes) throws RespException {
		List<RespClient> clients = new ArrayList<RespClient>();
		List<Exception> exceptions = new ArrayList<Exception>(); 
		for (ClusterNode node:nodes)
		{
			try {
				RespClient client = this.getInstanceFromNode(node);
				clients.add(client);
			} catch (IOException e) {
				exceptions.add(e);
			}			
		}
		if (exceptions.size() != 0)
		{
			throw new RespException("Cannot connect to nodes: " + exceptions);
		}
		return clients;
	}

}
