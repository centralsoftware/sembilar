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

package com.central.varth.resp.command;

import com.central.varth.resp.type.BulkString;
import com.central.varth.resp.type.RespArray;
import com.central.varth.resp.type.RespInteger;

public interface HashService extends CommandService {

	public RespInteger hset(String key, String field, String value);
	public BulkString hget(String key, String field);
	public RespArray hkeys(String key);
	public RespInteger hlen(String key);
}
