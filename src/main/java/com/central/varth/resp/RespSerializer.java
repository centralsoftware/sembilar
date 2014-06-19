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

import java.util.Arrays;
import java.util.List;


public class RespSerializer {

	public RespSerializer() {}
	
	public String serialize(String command, String... args)
	{
		StringBuilder cmd = new StringBuilder();
		String completeCmd = buildCompleteCommand(command, args);
		List<String> cmdList = tokenize(completeCmd);
		cmd.append(getHeader(cmdList));
		cmd.append(getBody(cmdList));
		return cmd.toString();
	}
	
	private String buildCompleteCommand(String command, String... args)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(command);
		for (int i=0;i<args.length;i++)
		{
			sb.append(ProtocolConstant.SPACE);
			sb.append(args[i]);
		}
		return sb.toString();
	}
	
	public List<String> tokenize(String command)
	{
		String[] arr = command.split(" ");
		return Arrays.asList(arr);
	}
	
	private String getHeader(List<String> cmdList)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(ProtocolConstant.COMMAND_ASTERISK);
		sb.append(cmdList.size());
		sb.append(ProtocolConstant.CRLF);
		return sb.toString();
	}
	
	private String getBody(List<String> cmdList)
	{
		StringBuffer sb = new StringBuffer();	
		for (int i=0;i<cmdList.size();i++)
		{
			sb.append(ProtocolConstant.COMMAND_DOLLAR);
			sb.append(cmdList.get(i).length());
			sb.append(ProtocolConstant.CRLF);
			sb.append(cmdList.get(i));
			sb.append(ProtocolConstant.CRLF);	
		}	
		return sb.toString();		
	}
	
}
