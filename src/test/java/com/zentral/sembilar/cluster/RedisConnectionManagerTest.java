package com.zentral.sembilar.cluster;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.zentral.sembilar.resp.AskException;
import com.zentral.sembilar.resp.MovedException;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.cluster.ClusterNode;
import com.zentral.sembilar.resp.command.ClusterCommand;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.connection.RespClient;
import com.zentral.sembilar.resp.connection.RespClientFactory;
import com.zentral.sembilar.resp.connection.impl.RedisConnectionManagerImpl;
import com.zentral.sembilar.resp.connection.impl.RedisRespClientFactory;
import com.zentral.sembilar.resp.connection.impl.RedisRespClientImpl;
import com.zentral.sembilar.resp.type.SimpleString;

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
	ClusterCommand clusterService;
	
	@Mock
	RespClient oldNodeClient;
	
	@Mock
	RespClient newNodeClient;
	
	@Mock
	RespClient normalClient;	
	
	InetSocketAddress address7001 = new InetSocketAddress("localhost", 7001);
	InetSocketAddress address7002 = new InetSocketAddress("localhost", 7002);	
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		
		endpoints.add(address7001);
		endpoints.add(address7002);
				
		client7001 = new RedisRespClientImpl("localhost", 7001, false);
		client7002 = new RedisRespClientImpl("localhost", 7002, false);
		client7003 = new RedisRespClientImpl("localhost", 7003, false);
		client7004 = new RedisRespClientImpl("localhost", 7004, false);
		client7005 = new RedisRespClientImpl("localhost", 7005, false);
		

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
	
	private void prepareInitialization() throws IOException, RespException
	{
		List<RespClient> clients = generateClients();
		when(factory.getInstanceFromAddress(any(InetSocketAddress.class))).thenReturn(client7001);
		when(factory.getInstanceFromNode(any(ClusterNode.class))).thenReturn(client7001);	
		when(factory.getInstancesFromNodes(anyListOf(ClusterNode.class))).thenReturn(clients);		
	}
	
	private void prepareMigratingClients() throws IOException, RespException
	{
		SimpleString okString = new SimpleString();
		okString.setString("OK");
		when(oldNodeClient.send(anyString(), eq(SimpleString.class))).thenThrow(new AskException("-ASK 3999 127.0.0.1:6381"));
		when(newNodeClient.send(anyString(), eq(SimpleString.class))).thenReturn(okString);
	}
	
	private void prepareMigratedClients() throws IOException, RespException
	{
		SimpleString okString = new SimpleString();
		okString.setString("OK");
		when(oldNodeClient.send(anyString(), eq(SimpleString.class))).thenThrow(new MovedException("-MOVED 3999 127.0.0.1:6381"));
		when(newNodeClient.send(anyString(), eq(SimpleString.class))).thenReturn(okString);
	}	
	
	private void prepareNormalClients() throws IOException, RespException
	{
		SimpleString okString = new SimpleString();
		okString.setString("OK");
		when(normalClient.send(anyString(), eq(SimpleString.class))).thenReturn(okString);
	}	
	
	@Test
	public void connectionManagerSuccessInitialization() throws RespException, IOException
	{		
		prepareInitialization();
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);
		List<RespClient> clients = connectionManager.getClients();
		Assert.assertEquals(5, clients.size());
	}
	
	@Test(expected = RespException.class)
	public void connectionManagerFailedInitialization() throws RespException, IOException
	{
		prepareInitialization();
		List<InetSocketAddress> endpoints = new ArrayList<InetSocketAddress>();
		endpoints.add(new InetSocketAddress("localhost", 7010));
		endpoints.add(new InetSocketAddress("localhost", 7011));
		
		RespClientFactory builder = new RedisRespClientFactory();
		connectionManager = new RedisConnectionManagerImpl(endpoints, builder, clusterService);
	}
	
	@Test
	public void findSlotTest() throws RespException, IOException
	{
		prepareInitialization();
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);
		int slot = connectionManager.findSlot("123456789");
		Assert.assertEquals(12739, slot);
	}
	
	@Test
	public void sendCommandWithoutKeyTest() throws IOException, RespException
	{
		prepareNormalClients();
		List<RespClient> clients = new ArrayList<RespClient>();
		clients.add(normalClient);
		when(factory.getInstancesFromNodes(anyListOf(ClusterNode.class))).thenReturn(clients);
		when(factory.getInstanceFromAddress(any(InetSocketAddress.class))).thenReturn(client7001);
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);
		SimpleString resp = connectionManager.send("PING", SimpleString.class);
		Assert.assertNotNull(resp);
	}
	
	@Test
	public void askRedirectionTest() throws IOException, RespException
	{
		prepareMigratingClients();
		List<RespClient> clients = new ArrayList<RespClient>();
		clients.add(oldNodeClient);
		when(factory.getInstancesFromNodes(anyListOf(ClusterNode.class))).thenReturn(clients);
		when(factory.getInstanceFromAddress(any(InetSocketAddress.class))).thenReturn(newNodeClient);
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);
		SimpleString resp = connectionManager.send("PING", SimpleString.class);
		Assert.assertNotNull(resp);
		verify(factory).getInstancesFromNodes(anyListOf(ClusterNode.class));
		verify(factory, times(3)).getInstanceFromAddress(any(InetSocketAddress.class));		
	}	
	
	@Test
	public void movedRedirectionTest() throws IOException, RespException
	{
		prepareMigratedClients();
		List<RespClient> clients = new ArrayList<RespClient>();
		clients.add(oldNodeClient);
		when(factory.getInstancesFromNodes(anyListOf(ClusterNode.class))).thenReturn(clients);
		when(factory.getInstanceFromAddress(any(InetSocketAddress.class))).thenReturn(newNodeClient);
		connectionManager = new RedisConnectionManagerImpl(endpoints, factory, clusterService);
		SimpleString resp = connectionManager.send("PING", SimpleString.class);
		Assert.assertNotNull(resp);
		verify(factory).getInstancesFromNodes(anyListOf(ClusterNode.class));
		verify(factory, times(3)).getInstanceFromAddress(any(InetSocketAddress.class));
	}		
	
}
