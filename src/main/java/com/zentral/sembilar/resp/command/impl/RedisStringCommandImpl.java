package com.zentral.sembilar.resp.command.impl;

import java.io.IOException;
import java.io.Serializable;

import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespCommandSerializer;
import com.zentral.sembilar.resp.RespDeserializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.RespSerializer;
import com.zentral.sembilar.resp.command.AbstractCommand;
import com.zentral.sembilar.resp.command.StringCommand;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.BulkString;
import com.zentral.sembilar.resp.type.SimpleString;

public class RedisStringCommandImpl extends AbstractCommand implements StringCommand
{

	private ConnectionManager connectionManager;
	
	@Override
	public boolean set(String key, String value) throws IOException, RespException {
		return set(key, value, false);
	}

	@Override
	public boolean set(String key, String value, boolean isOverWrite) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = "";
		if (isOverWrite)
		{
			cmd = serializer.serialize(ProtocolConstant.COMMAND_STRING_SET, key, value, ProtocolConstant.COMMAND_STRING_XX);		
		} else
		{
			cmd = serializer.serialize(ProtocolConstant.COMMAND_STRING_SET, key, value, ProtocolConstant.COMMAND_STRING_NX);					
		}
		SimpleString result = connectionManager.send(key, cmd, SimpleString.class);
		if (result.getString().equals(ProtocolConstant.RESPONSE_OK))
		{
			return true;
		}
		return false;
	}

	@Override
	public <T extends Serializable> boolean set(String key, T value,
			RespSerializer<T> serializer) throws IOException, RespException {
		String val = serializer.serialize(value);
		return set(key, val);
	}

	@Override
	public String get(String key) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_STRING_GET, key);	
		BulkString bulkString = connectionManager.send(key, cmd, BulkString.class);
		return bulkString.getString();
	}

	@Override
	public <T extends Serializable> T get(String key,
			RespDeserializer<T> deserializer) throws IOException, RespException {
		String value = get(key);
		return deserializer.deserialize(value);
	}

	@Override
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;	
	}

	
}
