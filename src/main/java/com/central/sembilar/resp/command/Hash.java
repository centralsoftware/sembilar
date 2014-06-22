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

package com.central.sembilar.resp.command;

import java.io.IOException;
import java.io.Serializable;

import com.central.sembilar.resp.RespDeserializer;
import com.central.sembilar.resp.RespException;
import com.central.sembilar.resp.RespSerializer;

public interface Hash extends Command {

	public int hset(String key, String field, String value) throws IOException, RespException;
	public <T extends Serializable> int hset(String key, String field, T value, RespSerializer<T> serializer) throws IOException, RespException;
	public String hget(String key, String field) throws IOException, RespException;
	public <T extends Serializable> T hget(String key, String field, RespDeserializer<T> deserializer) throws IOException, RespException;
	public java.util.List<String> hkeys(String key) throws IOException, RespException;
	public int hlen(String key) throws IOException, RespException;
}
