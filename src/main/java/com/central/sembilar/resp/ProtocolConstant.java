/**
 * 
 * Copyright ${year} Central Software

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
package com.central.sembilar.resp;

public class ProtocolConstant {
	
	public final static int SLOT_MAX_SIZE = 16384;
	
	public final static String CRLF = "\r\n";
	public final static String CR = "\r";
	public final static String LF = "\n";
	public final static String COMMAND_ASTERISK = "*";
	public final static String COMMAND_DOLLAR = "$";
	public final static String SPACE = " ";
	public final static String COLON = ":";
	public final static String SEMICOLON = ";";
	public final static String RESPONSE_OK = "OK";
	public final static String RESPONSE_MOVED = "MOVED";
	public final static String RESPONSE_ASK = "ASK";
	
	public final static String RESPONSE_PONG = "PONG";	
	
	// Connection commands
	public final static String COMMAND_AUTH = "AUTH";
	public final static String COMMAND_PING = "PING";
	public final static String COMMAND_SELECT = "SELECT";
	public final static String COMMAND_ECHO = "ECHO";	
	public final static String COMMAND_QUIT = "QUIT";
	
	// Cluster commands
	public final static String COMMAND_CLUSTER_INFO = "CLUSTER INFO";
	public final static String COMMAND_CLUSTER_NODES = "CLUSTER NODES";
	
	// Strings commands
	public final static String COMMAND_STRINGS_APPEND = "APPEND";
	public final static String COMMAND_STRINGS_SET = "SET";	
	public final static String COMMAND_STRINGS_GET = "GET";	
	
	// Lists commands
	public final static String COMMAND_LIST_RPUSH = "RPUSH";	
	public final static String COMMAND_LIST_LRANGE = "LRANGE";		
	
	// Hashes commands
	public final static String COMMAND_HASH_HSET = "HSET";	
	public final static String COMMAND_HASH_HGET = "HGET";	
	public final static String COMMAND_HASH_HKEYS = "HKEYS";		
	public final static String COMMAND_HASH_HLEN = "HLEN";		
	
}
