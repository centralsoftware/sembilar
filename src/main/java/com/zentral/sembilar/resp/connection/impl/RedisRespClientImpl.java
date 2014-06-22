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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import com.zentral.sembilar.resp.RespCommandDeserializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.cluster.ClusterNode;
import com.zentral.sembilar.resp.connection.RespClient;
import com.zentral.sembilar.resp.type.RespType;

public class RedisRespClientImpl implements RespClient {

	public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
	public static final int DEFAULT_SOCKET_TIMEOUT = 2000;
	public static final boolean DEFAULT_AUTOCONNECT = true;
	
	private String hostname;
	private int port;
	private ClusterNode clusterNode;
	private int connectTimeout;
	private int socketTimeout;
	private Socket socket = null;
	private OutputStream out;
	private InputStream in;
	
	
	public RedisRespClientImpl(String hostname, int port) throws IOException
	{
		this(hostname, port, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, DEFAULT_AUTOCONNECT);		
	}
	
	public RedisRespClientImpl(String hostname, int port, boolean autoConnect) throws IOException
	{
		this(hostname, port, DEFAULT_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, autoConnect);		
	}	

	public RedisRespClientImpl(ClusterNode node) throws IOException
	{
		this(node, true);
	}

	public RedisRespClientImpl(ClusterNode node, boolean autoConnect) throws IOException
	{
		this(node.getInetSocketAddress().getHostString(), node.getInetSocketAddress().getPort(), DEFAULT_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, autoConnect);		
		this.clusterNode = node;
	}		
	
	public RedisRespClientImpl(String hostname, int port, int connectTimout, int socketTimeout, boolean autoConnect) throws IOException
	{		
		this.setHostname(hostname);
		this.setPort(port);
		socket = new Socket();
		socket.setReuseAddress(true);
		socket.setKeepAlive(true); // Will monitor the TCP connection is valid
		socket.setTcpNoDelay(true); // Socket buffer Whetherclosed, to ensure timely delivery of data
		socket.setSoLinger(true, 0); // Control calls close () method, the underlying socket is closed immediately
		socket.setSoTimeout(socketTimeout);

		this.setConnectTimeout(connectTimout);
		this.setSocketTimeout(socketTimeout);

		if (autoConnect)
		{
			socket.connect(new InetSocketAddress(hostname, port), connectTimout);
			this.out = socket.getOutputStream();
			this.in = socket.getInputStream();	
		}
	}	
	
	@Override
	public void connect(String hostname, int port) throws IOException {
		if (!socket.isClosed())
		{
			socket.connect(new InetSocketAddress(hostname, port), this.getConnectTimeout());
		}					
	}
	
	@Override
	public void connect(ClusterNode node) throws IOException {
		this.connect(node.getInetSocketAddress().getHostName(), node.getInetSocketAddress().getPort());	
	}

	@Override
	public <T extends RespType> T send(String command, Class<T> responseClass) throws IOException, RespException {
		out.write(command.getBytes());
		socket.getOutputStream().flush();
		RespCommandDeserializer deserializer = new RespCommandDeserializer(in);
		T response = deserializer.deserialize(responseClass);
		return response;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	@Override
	public boolean hasSlot(int slot) {
		if (clusterNode != null)
		{
			List<String> slots = clusterNode.getSlots();
			for (String slotRange:slots)
			{
				if (isWithinRange(slot, slotRange))
				{
					return true;
				}
			}
		}
		return false;
	}

	private boolean isWithinRange(int slot, String slotRange)
	{ 
		String[] cols = slotRange.split("-");
		int start = 0;
		int end = 0;
		if (cols.length > 1)
		{
			start = Integer.parseInt(cols[0]);
			end = Integer.parseInt(cols[1]);
		} else
		{
			start = Integer.parseInt(slotRange);
			end = Integer.parseInt(slotRange);
		}
		if (slot >= start && slot <= end)
		{
			return true;
		}
		return false;
	}

	@Override
	public String toString()
	{
		return getClass().getName() + "["+hostname+", "+port+"]";
	}
}
