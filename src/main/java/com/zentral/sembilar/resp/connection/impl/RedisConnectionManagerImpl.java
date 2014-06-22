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

package com.zentral.sembilar.resp.connection.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.zentral.sembilar.resp.AskException;
import com.zentral.sembilar.resp.AskInfo;
import com.zentral.sembilar.resp.MovedException;
import com.zentral.sembilar.resp.MovedInfo;
import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.cluster.ClusterNode;
import com.zentral.sembilar.resp.command.ClusterCommand;
import com.zentral.sembilar.resp.command.impl.RedisClusterCommandImpl;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.connection.RespClient;
import com.zentral.sembilar.resp.connection.RespClientFactory;
import com.zentral.sembilar.resp.crc.CRC16;
import com.zentral.sembilar.resp.parser.RedirectionParser;
import com.zentral.sembilar.resp.parser.impl.RedisAskRedirectionParser;
import com.zentral.sembilar.resp.parser.impl.RedisMovedRedirectionParser;
import com.zentral.sembilar.resp.type.RespType;

public class RedisConnectionManagerImpl implements ConnectionManager 
{
		
	private final static long NODES_EXPIRY_PERIOD = 5*1000;
	
	private long lastNodesCreation = 0L;
	
	private CopyOnWriteArrayList<RespClient> nodeClients = new CopyOnWriteArrayList<RespClient>();
	
	private CopyOnWriteArrayList<RespClient> seedEndpointClients = new CopyOnWriteArrayList<RespClient>();

	private List<InetSocketAddress> seedEndpoints = new CopyOnWriteArrayList<InetSocketAddress>();

	private RespClientFactory respClientFactory = new RedisRespClientFactory();

	private ClusterCommand clusterService = new RedisClusterCommandImpl();	
	
	public RedisConnectionManagerImpl(List<InetSocketAddress> seedEndpoints, RespClientFactory factory, ClusterCommand clusterService) throws RespException, IOException
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

	protected void init() throws IOException, RespException 
	{
		if (!isNodesCreationExpired())
		{
			return;
		}		
		initSeeds();
		initNodes();		
		Date now = new Date();
		this.lastNodesCreation = now.getTime();		
	}

	private boolean isNodesCreationExpired()
	{
		Date now = new Date();
		if (now.getTime() - this.lastNodesCreation < NODES_EXPIRY_PERIOD)
		{
			return false;
		} else
		{
			return true;
		}
	}
	private void initSeeds() throws RespException {
		List<String> errors = new ArrayList<String>();
		for (InetSocketAddress address:this.seedEndpoints)
		{
			try {
				RespClient client = respClientFactory.getInstanceFromAddress(address);
				seedEndpointClients.addIfAbsent(client);
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

	private void initNodes() throws IOException, RespException
	{
		List<ClusterNode> nodes = clusterService.buildClusterNode();
		List<RespClient> clients = respClientFactory.getInstancesFromNodes(nodes);
		for (RespClient client:clients)
		{
			this.nodeClients.addIfAbsent(client);
		}
		if (nodeClients.size() == 0)
		{
			throw new RespException("Initialization failed, cannot find nodes");
		}		
	}

	@Override
	public <E extends RespType> E send(String command, Class<E> responseClass) throws IOException, RespException {
		RespClient client = getClientFromPool();
		E response = null;
		try {
			response = client.send(command, responseClass);
		} catch (RespException e) {
			response = handleRedirectException(e, command, responseClass);
		}
		return response;
	}
	
	@Override
	public <E extends RespType> E send(String key, String command, Class<E> responseClass) throws IOException, RespException {
		Integer slot = findSlot(key);
		RespClient client = findClient(slot);
		E response = null;
		try {
			response = client.send(command, responseClass);
		} catch (RespException e) {
			response = handleRedirectException(e, command, responseClass);
		}
		return response;
	}	
	
	private <E extends RespType> E handleRedirectException(RespException e, String command, Class<E> responseClass) throws RespException, IOException
	{
		if (e instanceof MovedException)
		{
			return handleMovedException((MovedException) e, command, responseClass);
		} else if (e instanceof AskException)
		{
			return handleAskException((AskException) e, command, responseClass); 
		} else
		{
			throw e;
		}
	}
	
	private <E extends RespType> E handleAskException(AskException e, String command, Class<E> responseClass) throws RespException, IOException
	{
		RespClient client = findClient(e);
		E resp = client.send(command, responseClass);
		return resp;
	}
	
	private RespClient findClient(AskException e) throws IOException
	{
		String message = e.getMessage();
		RedirectionParser<AskInfo> parser = new RedisAskRedirectionParser();
		AskInfo info = parser.parse(message);
		RespClient client = respClientFactory.getInstanceFromAddress(info.getAddress());
		return client;
	}
	
	private <E extends RespType> E handleMovedException(MovedException e, String command, Class<E> responseClass) throws RespException, IOException
	{
		RespClient client = findClient(e);
		E resp = client.send(command, responseClass);
		init();						
		return resp;		
	}
	
	private RespClient findClient(MovedException e) throws IOException
	{
		String message = e.getMessage();
		RedirectionParser<MovedInfo> parser = new RedisMovedRedirectionParser();
		MovedInfo info = parser.parse(message);
		RespClient client = respClientFactory.getInstanceFromAddress(info.getAddress());
		return client;
	}	
	
	@Override
	public RespClient getClientFromPool() {
		int index = getClientIndex();
		RespClient client = nodeClients.get(index);
		return client;
	}
	
	private int getClientIndex()
	{
		int max = nodeClients.size();
		double rand = (max-1)*Math.random();
		BigDecimal index = new BigDecimal(String.valueOf(rand));
		index = index.setScale(0, BigDecimal.ROUND_HALF_UP);
		return index.intValue();
	}

	@Override
	public List<RespClient> getSeedEndpointClients() {
		return this.seedEndpointClients;
	}
}
