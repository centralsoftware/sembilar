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

package com.central.varth.resp;


public enum Flags {
	MYSELF("myself"), MASTER("master"), SLAVE("slave"), FAIL("fail");
	String name;
	Flags(String name)
	{
		this.name = name;
	}
	String getName()
	{
		return this.name;
	}
	public static Flags fromName(String name)
	{
		if (name != null)
		{
			for (Flags flag:Flags.values())
			{
				if (name.equals(flag.getName()))
				{
					return flag;
				}
			}
		}
		return null;
	}
}
