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

package com.central.sembilar.resp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.central.sembilar.resp.RespDeserializer;
import com.central.sembilar.resp.RespException;
import com.central.sembilar.resp.type.BulkString;
import com.central.sembilar.resp.type.RespArray;
import com.central.sembilar.resp.type.SimpleString;

@Ignore
public class VarthClientTest {

	String hostname = "localhost";
	int port = 7004;
	Socket redisSocket = new Socket();		
	OutputStream out;
	InputStream in;

	@Before
	public void setUp() throws IOException
	{
		redisSocket.setReuseAddress(true);
		redisSocket.setKeepAlive(true); // Will monitor the TCP connection is
		// valid
		redisSocket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to
		// ensure timely delivery of data
		redisSocket.setSoLinger(true, 0); // Control calls close () method,
		// the underlying socket is closed
		// immediately
		redisSocket.connect(new InetSocketAddress(hostname, port), 2000);
		redisSocket.setSoTimeout(0);
		out = redisSocket.getOutputStream();
		in = redisSocket.getInputStream();
	}

	@After
	public void tearDown() throws IOException
	{
		out.close();
		in.close();
		redisSocket.close();
	}

	@Test
	public void pingTest() throws IOException, RespException
	{

		String command = "*1\r\n$4\r\nPING\r\n";
		out.write(command.getBytes());
		redisSocket.getOutputStream().flush();

		RespDeserializer deserializer = new RespDeserializer(in);
		SimpleString response = deserializer.deserialize(SimpleString.class);
		System.err.println(response);

		redisSocket.getOutputStream().write(command.getBytes());
		redisSocket.getOutputStream().flush();	

		response = deserializer.deserialize(SimpleString.class);
		System.err.println(response);			

	}
	
	@Test
	public void lrangeTest() throws IOException, RespException 
	{
		String command = "*4\r\n$6\r\nLRANGE\r\n$7\r\nmylist1\r\n$1\r\n0\r\n$2\r\n-1\r\n";
		out.write(command.getBytes());
		redisSocket.getOutputStream().flush();
		RespDeserializer deserializer = new RespDeserializer(in);
		
		RespArray array = deserializer.deserialize(RespArray.class);
		System.err.print(array);
	}
	
	@Test
	public void clusterInfoTest() throws IOException, RespException 
	{
		String command = "*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n";
		out.write(command.getBytes());
		redisSocket.getOutputStream().flush();
		RespDeserializer deserializer = new RespDeserializer(in);
		
		BulkString bulkString = deserializer.deserialize(BulkString.class);
		System.err.print(bulkString);
	}	
	
	@Test
	public void clusterNodesTest() throws IOException, RespException 
	{
		String command = "*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n";
		out.write(command.getBytes());
		redisSocket.getOutputStream().flush();
		RespDeserializer deserializer = new RespDeserializer(in);
		
		BulkString bulkString = deserializer.deserialize(BulkString.class);
		System.err.print(bulkString);
	}		
}
