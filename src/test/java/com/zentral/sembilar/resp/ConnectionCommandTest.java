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

package com.zentral.sembilar.resp;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespCommandSerializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.command.ConnectionCommand;
import com.zentral.sembilar.resp.command.impl.RedisConnectionCommandImpl;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.SimpleString;

public class ConnectionCommandTest {

	@Mock
	private ConnectionManager connectionManager;	
	
	private SimpleString simpleString = new SimpleString();
	private String password = "password";
	private String sentCommand = "";
	ConnectionCommand connectionCommand;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		connectionCommand = new RedisConnectionCommandImpl();
		connectionCommand.setConnectionManager(connectionManager);
	}
	
	private void prepAuth() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_AUTH, password);
		simpleString.setString(ProtocolConstant.RESPONSE_OK);
		when(connectionManager.send(sentCommand, SimpleString.class)).thenReturn(simpleString);
	}
	
	private void prepPing() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_PING);
		simpleString.setString(ProtocolConstant.RESPONSE_PONG);
		when(connectionManager.send(sentCommand, SimpleString.class)).thenReturn(simpleString);
	}	
	
	private void prepEcho(String message) throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_ECHO, message);
		simpleString.setString(message);
		when(connectionManager.send(sentCommand, SimpleString.class)).thenReturn(simpleString);
	}	
	
	@Test
	public void testAuth() throws IOException, RespException
	{		
		prepAuth();
		String response = connectionCommand.auth(password);
		Assert.assertNotNull(response);
		Assert.assertEquals(ProtocolConstant.RESPONSE_OK, response);
		verify(connectionManager).send(sentCommand, SimpleString.class);
	}
	
	@Test
	public void testPing() throws IOException, RespException
	{		
		prepPing();
		String response = connectionCommand.ping();
		Assert.assertNotNull(response);
		Assert.assertEquals(ProtocolConstant.RESPONSE_PONG, response);
		verify(connectionManager).send(sentCommand, SimpleString.class);
	}	
	
	@Test
	public void testEcho() throws IOException, RespException
	{		
		String message = "Hello World!";
		prepEcho(message);
		String response = connectionCommand.echo(message);
		Assert.assertNotNull(response);
		Assert.assertEquals(message, response);
		verify(connectionManager).send(sentCommand, SimpleString.class);
	}	
}
