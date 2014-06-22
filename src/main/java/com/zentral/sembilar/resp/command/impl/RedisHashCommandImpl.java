package com.zentral.sembilar.resp.command.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespCommandSerializer;
import com.zentral.sembilar.resp.RespDeserializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.RespSerializer;
import com.zentral.sembilar.resp.command.AbstractCommand;
import com.zentral.sembilar.resp.command.HashCommand;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.BulkString;
import com.zentral.sembilar.resp.type.RespArray;
import com.zentral.sembilar.resp.type.RespInteger;

public class RedisHashCommandImpl extends AbstractCommand implements HashCommand
{

	private ConnectionManager connectionManager;	
	
	@Override
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;		
	}

	@Override
	public int hset(String key, String field, String value) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_HASH_HSET, key, field, value);
		RespInteger resp = connectionManager.send(key, cmd, RespInteger.class);
		return resp.getInteger();
	}

	@Override
	public String hget(String key, String field) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_HASH_HGET, key, field);
		BulkString resp = connectionManager.send(key, cmd, BulkString.class);
		return resp.getString();
	}

	@Override
	public List<String> hkeys(String key) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_HASH_HKEYS, key);
		RespArray resp = connectionManager.send(key, cmd, RespArray.class);
		List<String> strings = convertElementToString(resp);
		return strings;
	}

	@Override
	public int hlen(String key) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_HASH_HLEN, key);
		RespInteger resp = connectionManager.send(key, cmd, RespInteger.class);
		return resp.getInteger();
	}

	@Override
	public <T extends Serializable> T hget(String key, String field,
			RespDeserializer<T> deserializer)
			throws IOException, RespException {
		String value = hget(key, field);
		T object = deserializer.deserialize(value);
		return object;
	}

	@Override
	public <T extends Serializable> int hset(String key, String field, T value,
			RespSerializer<T> serializer) throws IOException, RespException {
		String val = serializer.serialize(value);
		int res = hset(key, field, val);
		return res;
	}


}
