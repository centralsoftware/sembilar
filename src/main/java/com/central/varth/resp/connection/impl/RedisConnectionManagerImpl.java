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

package com.central.varth.resp.connection.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.central.varth.resp.ProtocolConstant;
import com.central.varth.resp.RespException;
import com.central.varth.resp.cluster.ClusterNode;
import com.central.varth.resp.cluster.ClusterNodeParser;
import com.central.varth.resp.cluster.RedisClusterNodeParser;
import com.central.varth.resp.command.ClusterService;
import com.central.varth.resp.command.impl.RedisClusterServiceImpl;
import com.central.varth.resp.connection.ConnectionManager;
import com.central.varth.resp.connection.RespClient;
import com.central.varth.resp.connection.RespClientFactory;
import com.central.varth.resp.crc.CRC16;
import com.central.varth.resp.type.BulkString;
import com.central.varth.resp.type.RespType;

public class RedisConnectionManagerImpl implements ConnectionManager 
{
	
	private List<RespClient> nodeClients = new ArrayList<RespClient>();
	
	private List<RespClient> seedEndpointClients = new ArrayList<RespClient>();

	private List<InetSocketAddress> seedEndpoints = new ArrayList<InetSocketAddress>();

	private RespClientFactory respClientFactory = new RedisRespClientFactory();

	private ClusterService clusterService = new RedisClusterServiceImpl();	
	
	public RedisConnectionManagerImpl(List<InetSocketAddress> seedEndpoints, RespClientFactory factory, ClusterService clusterService) throws RespException
	{
		this.seedEndpoints = seedEndpoints;
		this.respClientFactory = factory;
		this.clusterService = clusterService;
		this.clusterService.setConnectionManager(this);
		init();
	}
	
	@Override
	public Integer findSlot(String key) {
		int crc = CRC16.checksum(key);
		int slot = crc % ProtocolConstant.SLOT_MAX_SIZE;
		return slot;
	}

	@Override
	public RespClient findClient(Integer slot) {
		RespClient res = null;
		for (RespClient client:nodeClients)
		{
			if (client.hasSlot(slot))
			{
				res = client;
				break;
			}
		}
		return res;
	}

	protected void init() throws RespException 
	{
		this.seedEndpointClients = new ArrayList<RespClient>();
		initSeedClients();
		
		List<String> errors = new ArrayList<String>();
		for (RespClient seedClient:seedEndpointClients)
		{
			try {
				nodeClients = buildAllClients(seedClient);
				break;
			} catch (IOException | RespException e) {
				errors.add("Failed to contact " + seedClient + " with error " + e.getMessage());
			}
		}
		if (nodeClients.size() == 0)
		{
			throw new RespException("Initialization failed due to: " + errors);
		}
	}

	private void initSeedClients() throws RespException {
		List<String> errors = new ArrayList<String>();
		for (InetSocketAddress address:this.seedEndpoints)
		{
			try {
				RespClient client = respClientFactory.getInstanceFromAddress(address);
				seedEndpointClients.add(client);
			} catch (IOException e) {
				errors.add("Failed during connect to: " + address);
			}			
		}
		if (errors.size() != 0)
		{
			throw new RespException("Initialization failed due to: " + errors);
		}
	}
		
	public List<RespClient> getClients() {
		return nodeClients;
	}

	private List<RespClient> buildAllClients(RespClient seedClient) throws IOException, RespException
	{
		List<RespClient> clients = new ArrayList<RespClient>();
		List<ClusterNode> nodes = clusterService.buildClusterNode();
		clients = respClientFactory.getInstancesFromNodes(nodes);
		return clients;
	}

	@Override
	public <E extends RespType> E send(String command, Class<E> responseClass)
			throws IOException, RespException {
		RespClient client = getClientFromPool();
		E response = client.send(command, responseClass);
		return response;
	}
	
	@Override
	public <E extends RespType> E send(String key, String command, Class<E> responseClass)
			throws IOException, RespException {
		Integer slot = findSlot(key);
		RespClient client = findClient(slot);
		E response = client.send(command, responseClass);		
		return response;
	}	

	public void setSeedEndpoints(List<InetSocketAddress> seedEndpoints) {
		this.seedEndpoints = seedEndpoints;
	}

	@Override
	public RespClient getClientFromPool() {
		RespClient client = nodeClients.get(0);
		return client;
	}

	@Override
	public List<RespClient> getSeedEndpointClients() {
		return this.seedEndpointClients;
	}
}
