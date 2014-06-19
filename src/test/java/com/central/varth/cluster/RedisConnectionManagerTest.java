package com.central.varth.cluster;

import static org.mockito.Matchers.any;
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
import com.central.varth.resp.cluster.ClusterNodeParser;
import com.central.varth.resp.cluster.RedisClusterNodeParser;
import com.central.varth.resp.command.ClusterService;
import com.central.varth.resp.connection.ConnectionManager;
import com.central.varth.resp.connection.RespClient;
import com.central.varth.resp.connection.RespClientFactory;
import com.central.varth.resp.connection.impl.RedisConnectionManagerImpl;
import com.central.varth.resp.connection.impl.RedisRespClientFactory;
import com.central.varth.resp.connection.impl.RedisRespClientImpl;

public class RedisConnectionManagerTest {

	private ConnectionManager connectionManager;

	private String rawClusterInfo = "1350e2bb7b20f160e54629958b4dabb772c661b0 127.0.0.1:7002 master - 0 1402694957597 22 connected 0-565 5962-10922 11422-11855\n" + 
			"d8806a2ec150cc0153b4ea16d837e7958f81b4cb 127.0.0.1:7006 slave 35919c478bfd35677d33902badc2508dc51776d0 0 1402694959106 21 connected\n" + 
			"ddebc4aeb5a96b5ceffd0d64c571c3885c2e7ded :0 myself,slave 7ce9cdc2c8dec44a0f09096af6f8f9865257e724 0 0 19 connected\n" + 
			"35919c478bfd35677d33902badc2508dc51776d0 127.0.0.1:7003 master - 0 1402694958605 21 connected 11856-16383\n" + 
			"6ab17726d88c1fdf8693b22b25aef8ba0e806efa 127.0.0.1:7005 slave 1350e2bb7b20f160e54629958b4dabb772c661b0 0 1402694958101 22 connected\n" + 
			"7ce9cdc2c8dec44a0f09096af6f8f9865257e724 127.0.0.1:7001 master - 0 1402694957597 23 connected 566-5961 10923-11421";
	
	private RespClient client7001;
	private RespClient client7002;
	private RespClient client7003;
	private RespClient client7004;
	private RespClient client7005;	
	
	@Mock
	RespClientFactory factory;
	
	@Mock
	ClusterService clusterService;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		
		client7001 = new RedisRespClientImpl("localhost", 7001, false);
		client7002 = new RedisRespClientImpl("localhost", 7002, false);
		client7003 = new RedisRespClientImpl("localhost", 7003, false);
		client7004 = new RedisRespClientImpl("localhost", 7004, false);
		client7005 = new RedisRespClientImpl("localhost", 7005, false);
		
		
		ClusterNodeParser parser = new RedisClusterNodeParser();
		List<ClusterNode> nodes = parser.parse(rawClusterInfo);
		when(clusterService.buildClusterNode()).thenReturn(nodes);
		when(factory.getInstanceFromAddress(any(InetSocketAddress.class))).thenReturn(client7001);
		when(factory.getInstanceFromNode(any(ClusterNode.class))).thenReturn(client7001);	
		when(factory.getInstancesFromNodes(nodes)).thenReturn(generateClients());
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
		List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();
		endpoints.add(new InetSocketAddress("localhost", 7001));
		endpoints.add(new InetSocketAddress("localhost", 7002));
		
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
	
}
