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

package com.zentral.sembilar.resp.command.impl;

import java.io.IOException;

import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespCommandSerializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.command.ConnectionCommand;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.SimpleString;

public class RedisConnectionCommandImpl implements ConnectionCommand
{

	private ConnectionManager connectionManager;
	
	@Override
	public String auth(String password) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_AUTH, password);
		SimpleString resp = connectionManager.send(cmd, SimpleString.class);
		return resp.getString();
	}
	
	@Override
	public String ping() throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_PING);
		SimpleString resp = connectionManager.send(cmd, SimpleString.class);
		return resp.getString();
	}

	@Override
	public String echo(String message) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_ECHO, message);
		SimpleString resp = connectionManager.send(cmd, SimpleString.class);
		return resp.getString();
	}

	@Override
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}
}
