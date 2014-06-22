package com.zentral.sembilar.resp.command;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.zentral.sembilar.resp.RespSerializer;
import com.zentral.sembilar.resp.type.BulkString;
import com.zentral.sembilar.resp.type.RespArray;
import com.zentral.sembilar.resp.type.RespInteger;
import com.zentral.sembilar.resp.type.RespType;
import com.zentral.sembilar.resp.type.SimpleString;

public abstract class AbstractCommand implements Command
{

	protected String[] convertKeyValuesToArray(String key, List<String> values)
	{
		String[] result = new String[values.size() + 1];
		result[0] = key;
		for (int i=1;i<result.length;i++)
		{
			result[i] = values.get(i-1);
		}
		return result;
	}	
	
	protected <T extends Serializable> String[] convertKeyValuesToArray(String key, RespSerializer<T> serializer, List<T> values)
	{
		String[] result = new String[values.size() + 1];
		result[0] = key;
		for (int i=1;i<result.length;i++)
		{
			String val = serializer.serialize(values.get(i-1));
			result[i] = val;
		}
		return result;
	}		
	
	protected List<String> convertElementToString(RespArray respArray)
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
	
	
}
