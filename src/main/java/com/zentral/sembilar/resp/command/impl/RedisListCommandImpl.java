package com.zentral.sembilar.resp.command.impl;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespCommandSerializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.RespSerializer;
import com.zentral.sembilar.resp.command.AbstractCommand;
import com.zentral.sembilar.resp.command.ListCommand;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.RespArray;
import com.zentral.sembilar.resp.type.RespInteger;

public class RedisListCommandImpl extends AbstractCommand implements ListCommand {

	private ConnectionManager connectionManager;
	
	@Override
	public void setConnectionManager(ConnectionManager connectionManager) {
		this.connectionManager = connectionManager;		
	}

	@Override
	public int rpush(String key, String value) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_LIST_RPUSH, key, value);		
		RespInteger result = connectionManager.send(key, cmd, RespInteger.class);
		return result.getInteger();
	}

	@Override
	public int rpush(String key, List<String> values) throws IOException, RespException {
		String[] vals = convertKeyValuesToArray(key, values);
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_LIST_RPUSH, vals);		
		RespInteger result = connectionManager.send(key, cmd, RespInteger.class);
		return result.getInteger();
	}

	@Override
	public <T extends Serializable> int rpush(String key,
			RespSerializer<T> serializer, T value) throws IOException, RespException {
		String val = serializer.serialize(value);
		int result = rpush(key, val);
		return result;
	}

	@Override
	public <T extends Serializable> int rpush(String key,
			RespSerializer<T> serializer, List<T> values) throws IOException, RespException {
		List<String> list = new ArrayList<String>();
		for (T value:values)
		{
			list.add(serializer.serialize(value));
		}
		int result = rpush(key, list);
		return result;
	}

	@Override
	public List<String> lrange(String key) throws IOException, RespException {
		return lrange(key, 0, -1);
	}

	@Override
	public int llen(String key) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_LIST_LLEN, key);		
		RespInteger result = connectionManager.send(key, cmd, RespInteger.class);
		return result.getInteger();
	}

	@Override
	public List<String> lrange(String key, int start, int stop) throws IOException, RespException {
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmd = serializer.serialize(ProtocolConstant.COMMAND_LIST_LRANGE, key, Integer.toString(start), Integer.toString(stop));		
		RespArray result = connectionManager.send(key, cmd, RespArray.class);
		List<String> list = convertElementToString(result);
		return list;
	}

}
