package com.central.sembilar.resp;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.central.sembilar.resp.command.HashCommand;
import com.central.sembilar.resp.command.impl.RedisHashCommandImpl;
import com.central.sembilar.resp.connection.ConnectionManager;
import com.central.sembilar.resp.type.BulkString;
import com.central.sembilar.resp.type.RespArray;
import com.central.sembilar.resp.type.RespInteger;
import com.central.sembilar.resp.type.RespType;
import com.central.sembilar.resp.type.SimpleString;

public class HashCommandTest {

	@Mock
	private ConnectionManager connectionManager;	
	
	private String sentCommand = "";
	private RespInteger resultOkInt;
	private BulkString bulkStringResp;
	private RespArray array;
	private HashCommand hashCommand;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		hashCommand = new RedisHashCommandImpl();
		hashCommand.setConnectionManager(connectionManager);
	}
	
	private void prepHset() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_HASH_HSET, "key", "field", "value");
		resultOkInt = new RespInteger();
		resultOkInt.setInteger(1);
		when(connectionManager.send("key", sentCommand, RespInteger.class)).thenReturn(resultOkInt);
	}
	
	@Test
	public void hsetTest() throws IOException, RespException
	{
		prepHset();
		int i = hashCommand.hset("key", "field", "value");
		Assert.assertEquals(1, i);
		verify(connectionManager).send("key", sentCommand, RespInteger.class);		
	}
	
	private void prepHsetReturnZero() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_HASH_HSET, "key", "field", "value");
		resultOkInt = new RespInteger();
		resultOkInt.setInteger(0);
		when(connectionManager.send("key", sentCommand, RespInteger.class)).thenReturn(resultOkInt);
	}
	
	@Test
	public void hsetTestReturnZero() throws IOException, RespException
	{
		prepHsetReturnZero();
		int i = hashCommand.hset("key", "field", "value");
		Assert.assertEquals(0, i);
		verify(connectionManager).send("key", sentCommand, RespInteger.class);		
	}	
	
	private void prepHget() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_HASH_HGET, "key", "field");
		bulkStringResp = new BulkString();
		bulkStringResp.setSize(3);
		bulkStringResp.setString("foo");
		when(connectionManager.send("key", sentCommand, BulkString.class)).thenReturn(bulkStringResp);
	}	

	@Test
	public void hgetTest() throws IOException, RespException
	{
		prepHget();
		String resp = hashCommand.hget("key", "field");
		Assert.assertEquals(resp, "foo");
		verify(connectionManager).send("key", sentCommand, BulkString.class);
	}	
	
	private void prepHgetReturnNull() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_HASH_HGET, "key", "field");
		bulkStringResp = new BulkString();
		bulkStringResp.setSize(-1);
		bulkStringResp.setString(null);
		when(connectionManager.send("key", sentCommand, BulkString.class)).thenReturn(bulkStringResp);
	}	
	
	@Test
	public void hgetReturnNullTest() throws IOException, RespException
	{
		prepHgetReturnNull();
		String resp = hashCommand.hget("key", "field");
		Assert.assertEquals(resp, null);
		verify(connectionManager).send("key", sentCommand, BulkString.class);
	}	
	
	private void prepHkeys() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_HASH_HKEYS, "key");
		array = new RespArray();
		array.setSize(2);
		List<RespType> list = new ArrayList<RespType>();
		SimpleString ok = new SimpleString();
		ok.setString("OK");
		RespInteger ten = new RespInteger();
		ten.setInteger(10);
		list.add(ok);
		list.add(ten);
		array.setElement(list);
		when(connectionManager.send("key", sentCommand, RespArray.class)).thenReturn(array);
	}	
	
	@Test
	public void hkeysTest() throws IOException, RespException
	{
		prepHkeys();
		List<String> resp = hashCommand.hkeys("key");
		Assert.assertNotNull(resp);
		Assert.assertEquals(2, resp.size());
		verify(connectionManager).send("key", sentCommand, RespArray.class);
	}		
}
