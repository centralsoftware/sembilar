package com.zentral.sembilar.resp;

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

import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.RespCommandSerializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.command.ListCommand;
import com.zentral.sembilar.resp.command.impl.RedisListCommandImpl;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.RespArray;
import com.zentral.sembilar.resp.type.RespInteger;
import com.zentral.sembilar.resp.type.RespType;
import com.zentral.sembilar.resp.type.SimpleString;

public class ListCommandTest {

	@Mock
	private ConnectionManager connectionManager;	

	private String sentCommand = "";
	private ListCommand listCommand;
	
	private RespInteger resultInt;
	private RespArray array;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		listCommand = new RedisListCommandImpl();
		listCommand.setConnectionManager(connectionManager);
	}
	
	private void prepLlen() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_LIST_LLEN, "key");		
		resultInt = new RespInteger();
		resultInt.setInteger(1);
		when(connectionManager.send("key", sentCommand, RespInteger.class)).thenReturn(resultInt);
	}
	
	@Test
	public void llenTest() throws IOException, RespException
	{
		prepLlen();
		int result = listCommand.llen("key");
		Assert.assertEquals(1, result);
		verify(connectionManager).send("key", sentCommand, RespInteger.class);
	}
	
	private void prepRpush() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_LIST_RPUSH, "key", "value");		
		resultInt = new RespInteger();
		resultInt.setInteger(1);
		when(connectionManager.send("key", sentCommand, RespInteger.class)).thenReturn(resultInt);
	}
	
	@Test
	public void rpushTest() throws IOException, RespException
	{
		prepRpush();
		int result = listCommand.rpush("key", "value");
		Assert.assertEquals(1, result);
		verify(connectionManager).send("key", sentCommand, RespInteger.class);
	}
	
	private void prepMultiValuesRpush() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_LIST_RPUSH, "key", "value1", "value2");		
		resultInt = new RespInteger();
		resultInt.setInteger(2);
		when(connectionManager.send("key", sentCommand, RespInteger.class)).thenReturn(resultInt);
	}	
	
	@Test
	public void multiValueRpushTest() throws IOException, RespException
	{
		prepMultiValuesRpush();
		List<String> list = new ArrayList<String>();
		list.add("value1");
		list.add("value2");
		int result = listCommand.rpush("key", list);
		Assert.assertEquals(2, result);
		verify(connectionManager).send("key", sentCommand, RespInteger.class);
	}
	
	private void prepLrange() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_LIST_LRANGE, "key", "0", "1");		
		array = new RespArray();
		List<RespType> list = new ArrayList<RespType>();
		list.add(new SimpleString("value1"));
		list.add(new SimpleString("value2"));		
		array.setElement(list);
		when(connectionManager.send("key", sentCommand, RespArray.class)).thenReturn(array);
	}	
	
	@Test
	public void lrangeTest() throws IOException, RespException
	{
		prepLrange();
		List<String> result = listCommand.lrange("key", 0, 1);
		Assert.assertEquals(2, result.size());
		verify(connectionManager).send("key", sentCommand, RespArray.class);
	}	
}
