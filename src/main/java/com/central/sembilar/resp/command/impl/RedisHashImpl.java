package com.central.sembilar.resp.command.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.central.sembilar.resp.ProtocolConstant;
import com.central.sembilar.resp.RespCommandSerializer;
import com.central.sembilar.resp.RespDeserializer;
import com.central.sembilar.resp.RespException;
import com.central.sembilar.resp.RespSerializer;
import com.central.sembilar.resp.command.Hash;
import com.central.sembilar.resp.connection.ConnectionManager;
import com.central.sembilar.resp.type.BulkString;
import com.central.sembilar.resp.type.RespArray;
import com.central.sembilar.resp.type.RespInteger;
import com.central.sembilar.resp.type.RespType;
import com.central.sembilar.resp.type.SimpleString;

public class RedisHashImpl implements Hash
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
	
	private List<String> convertElementToString(RespArray respArray)
	{
		List<String> strings = new ArrayList<String>();
		List<RespType> list = respArray.getElement();
		for (RespType type:list)
		{
			if (type instanceof RespInteger)
			{
				int i = ((RespInteger)type).getInteger();
				strings.add(Integer.toString(i));
			} else if (type instanceof SimpleString)
			{
				String str = ((SimpleString) type).getString();
				strings.add(str);
			} else if (type instanceof BulkString)
			{
				String str = ((BulkString) type).getString();
				strings.add(str);
			} else if (type instanceof RespArray)
			{
				List<String> arr = convertElementToString((RespArray)type);
				strings.addAll(arr);
			} else
			{
				throw new RuntimeException("Cannot detect element with type " + type.getClass().getName());
			}
		}
		return strings;
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
