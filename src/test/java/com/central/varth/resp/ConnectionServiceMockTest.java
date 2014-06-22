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

package com.central.varth.resp;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.central.sembilar.resp.command.impl.ConnectionServiceImpl;
import com.central.varth.resp.command.ConnectionService;
import com.central.varth.resp.connection.ConnectionManager;
import com.central.varth.resp.type.SimpleString;

public class ConnectionServiceMockTest {

	@Mock
	private ConnectionManager connectionManager;	
	
	private SimpleString simpleString = new SimpleString();
	private String password = "password";
	private String sentCommand = "";
	ConnectionService connectionService;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		connectionService = new ConnectionServiceImpl();
		connectionService.setConnectionManager(connectionManager);
	}
	
	private void prepAuth() throws IOException, RespException
	{
		RespSerializer serializer = new RespSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_AUTH, password);
		simpleString.setString(ProtocolConstant.RESPONSE_OK);
		when(connectionManager.send(sentCommand, SimpleString.class)).thenReturn(simpleString);
	}
	
	private void prepPing() throws IOException, RespException
	{
		RespSerializer serializer = new RespSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_PING);
		simpleString.setString(ProtocolConstant.RESPONSE_PONG);
		when(connectionManager.send(sentCommand, SimpleString.class)).thenReturn(simpleString);
	}	
	
	private void prepEcho(String message) throws IOException, RespException
	{
		RespSerializer serializer = new RespSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_ECHO, message);
		simpleString.setString(message);
		when(connectionManager.send(sentCommand, SimpleString.class)).thenReturn(simpleString);
	}	
	
	@Test
	public void testAuth() throws IOException, RespException
	{		
		prepAuth();
		String response = connectionService.auth(password);
		Assert.assertNotNull(response);
		Assert.assertEquals(ProtocolConstant.RESPONSE_OK, response);
		verify(connectionManager).send(sentCommand, SimpleString.class);
	}
	
	@Test
	public void testPing() throws IOException, RespException
	{		
		prepPing();
		String response = connectionService.ping();
		Assert.assertNotNull(response);
		Assert.assertEquals(ProtocolConstant.RESPONSE_PONG, response);
		verify(connectionManager).send(sentCommand, SimpleString.class);
	}	
	
	@Test
	public void testEcho() throws IOException, RespException
	{		
		String message = "Hello World!";
		prepEcho(message);
		String response = connectionService.echo(message);
		Assert.assertNotNull(response);
		Assert.assertEquals(message, response);
		verify(connectionManager).send(sentCommand, SimpleString.class);
	}	
}
