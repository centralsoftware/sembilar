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

package com.zentral.sembilar.resp.command;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import com.zentral.sembilar.resp.RespException;
import com.zentral.sembilar.resp.RespSerializer;

public interface ListCommand extends Command {

	public int rpush(String key, String value) throws IOException, RespException;
	public int rpush(String key, List<String> values) throws IOException, RespException;
	public <T extends Serializable> int rpush(String key, RespSerializer<T> serializer, T value) throws IOException, RespException;
	public <T extends Serializable> int rpush(String key, RespSerializer<T> serializer, List<T> values) throws IOException, RespException;
	public List<String> lrange(String key, int start, int stop) throws IOException, RespException;
	public List<String> lrange(String key) throws IOException, RespException;
	public int llen(String key) throws IOException, RespException;
}
