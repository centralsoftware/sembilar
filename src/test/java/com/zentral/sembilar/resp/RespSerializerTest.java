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

package com.zentral.sembilar.resp;

import org.junit.Assert;
import org.junit.Test;

import com.zentral.sembilar.resp.RespCommandSerializer;

public class RespSerializerTest {

	@Test
	public void pingCommand()
	{
		String command = "PING";
		String expCommand = "*1\r\n$4\r\nPING\r\n";
		RespCommandSerializer serializer = new RespCommandSerializer();
		String cmdz = serializer.serialize(command);
		System.err.println(cmdz);
		Assert.assertEquals(expCommand, cmdz);		
	}
}
