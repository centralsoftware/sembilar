package com.zentral.sembilar.resp.command;

import java.io.IOException;
import java.io.Serializable;

import com.zentral.sembilar.resp.RespDeserializer;
import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.RespSerializer;

public interface StringCommand extends Command {	
	public boolean set(String key, String value) throws IOException, RespException;
	public <T extends Serializable> boolean set(String key, T value, RespSerializer<T> serializer) throws IOException, RespException;
	public boolean set(String key, String value, boolean isOverWrite) throws IOException, RespException;
	public String get(String key) throws IOException, RespException;
	public <T extends Serializable> T get(String key, RespDeserializer<T> deserializer) throws IOException, RespException;
}
