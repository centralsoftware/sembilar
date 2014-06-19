package com.central.varth.cluster;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.central.varth.resp.RespException;
import com.central.varth.resp.cluster.ClusterNode;
import com.central.varth.resp.command.ClusterService;
import com.central.varth.resp.connection.ConnectionManager;
import com.central.varth.resp.connection.RespClient;
import com.central.varth.resp.connection.RespClientFactory;
import com.central.varth.resp.connection.impl.RedisConnectionManagerImpl;
import com.central.varth.resp.connection.impl.RedisRespClientFactory;
import com.central.varth.resp.connection.impl.RedisRespClientImpl;

public class RedisConnectionManagerTest {

	private ConnectionManager connectionManager;

	private RespClient client7001;
	private RespClient client7002;
	private RespClient client7003;
	private RespClient client7004;
	private RespClient client7005;	
	
	List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();	
	
	@Mock
	RespClientFactory factory;
	
	@Mock
	ClusterService clusterService;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		List<RespClient> clients = generateClients();
		endpoints.add(new InetSocketAddress("localhost", 7001));
		endpoints.add(new InetSocketAddress("localhost", 7002));
		
		
		client7001 = new RedisRespClientImpl("localhost", 7001, false);
		client7002 = new RedisRespClientImpl("localhost", 7002, false);
		client7003 = new RedisRespClientImpl("localhost", 7003, false);
		client7004 = new RedisRespClientImpl("localhost", 7004, false);
		client7005 = new RedisRespClientImpl("localhost", 7005, false);
		
		when(factory.getInstanceFromAddress(any(InetSocketAddress.class))).thenReturn(client7001);
		when(factory.getInstanceFromNode(any(ClusterNode.class))).thenReturn(client7001);	
		when(factory.getInstancesFromNodes(anyListOf(ClusterNode.class))).thenReturn(clients);
	}
	
	private List<RespClient> generateClients()
	{
		List<RespClient> clients = new ArrayList<RespClient>();
		clients.add(client7001);
		clients.add(client7002);
		clients.add(client7003);
		clients.add(client7004);
		clients.add(client7005);		
		return clients;
	}
	
	@Test
	public void connectionManagerSuccessInitialization() throws RespException
	{		
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);		
		List<RespClient> clients = connectionManager.getClients();
		Assert.assertEquals(5, clients.size());
	}
	
	@Test(expected = RespException.class)
	public void connectionManagerFailedInitialization() throws RespException
	{
		List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();
		endpoints.add(new InetSocketAddress("localhost", 7010));
		endpoints.add(new InetSocketAddress("localhost", 7011));
		
		RespClientFactory builder = new RedisRespClientFactory();
		connectionManager = new RedisConnectionManagerImpl(endpoints, builder, clusterService);		
	}
	
	@Test
	public void findSlotTest() throws RespException
	{
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);		
		int slot = connectionManager.findSlot("123456789");
		Assert.assertEquals(12739, slot);
	}
	
}
