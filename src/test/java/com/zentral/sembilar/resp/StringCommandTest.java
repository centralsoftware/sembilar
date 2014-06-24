package com.zentral.sembilar.resp;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.zentral.sembilar.resp.command.StringCommand;
import com.zentral.sembilar.resp.command.impl.RedisStringCommandImpl;
import com.zentral.sembilar.resp.connection.ConnectionManager;
import com.zentral.sembilar.resp.type.BulkString;
import com.zentral.sembilar.resp.type.SimpleString;

public class StringCommandTest {

	@Mock
	private ConnectionManager connectionManager;	

	private String sentCommand = "";
	private StringCommand stringCommand;
	
	private SimpleString resultString;
	private BulkString result;
	
	@Before
	public void setUp() throws IOException, RespException
	{
		MockitoAnnotations.initMocks(this);
		stringCommand = new RedisStringCommandImpl();
		stringCommand.setConnectionManager(connectionManager);
	}	
	
	private void prepSet() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_STRINGS_SET, "key", "value", ProtocolConstant.COMMAND_STRING_NX);				
		resultString = new SimpleString(ProtocolConstant.RESPONSE_OK);
		when(connectionManager.send("key", sentCommand, SimpleString.class)).thenReturn(resultString);
	}
	
	@Test
	public void setTest() throws IOException, RespException
	{
		prepSet();
		boolean result = stringCommand.set("key", "value", false);
		Assert.assertEquals(result, true);
		verify(connectionManager).send("key", sentCommand, SimpleString.class);
	}
	
	private void prepGet() throws IOException, RespException
	{
		RespCommandSerializer serializer = new RespCommandSerializer();
		sentCommand = serializer.serialize(ProtocolConstant.COMMAND_STRINGS_GET, "key");				
		result = new BulkString();
		result.setString("value");
		when(connectionManager.send("key", sentCommand, BulkString.class)).thenReturn(result);
	}
	
	@Test
	public void getTest() throws IOException, RespException
	{
		prepGet();
		String res = stringCommand.get("key");
		Assert.assertEquals(res, "value");
		verify(connectionManager).send("key", sentCommand, BulkString.class);
	}	
}
