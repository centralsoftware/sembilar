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

package com.central.sembilar.resp.parser.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.central.sembilar.resp.Flags;
import com.central.sembilar.resp.ProtocolConstant;
import com.central.sembilar.resp.cluster.ClusterNode;
import com.central.sembilar.resp.parser.ClusterNodeParser;

public class RedisClusterNodeParser implements ClusterNodeParser 
{

	public List<ClusterNode> parse(String raw)
	{
		String[] arr = raw.split(ProtocolConstant.LF);
		List<ClusterNode> nodes = new ArrayList<ClusterNode>();
		for (int i=0;i<arr.length;i++)
		{
			ClusterNode node = parseLine(arr[i]);
			nodes.add(node);
		}
		return nodes;
	}
	
	protected ClusterNode parseLine(String line)
	{
		ClusterNode node = new ClusterNode();
		String[] cols = line.split(ProtocolConstant.SPACE);
		node.setNodeId(cols[0]);
		node.setInetSocketAddress(buildInetSocketAddress(cols[1]));
		node.setFlags(Flags.fromName(cols[2]));
		node.setMasterNodeId(cols[3]);
		node.setLastPendingPing(cols[4]);
		node.setLastPongReceived(cols[5]);
		node.setEpoch(cols[6]);
		node.setStatus(cols[7]);
		List<String> slots = new ArrayList<String>();
		for (int i=8;i<cols.length;i++)
		{
			slots.add(cols[i]);
		}
		node.setSlots(slots);
		return node;
	}
	
	private InetSocketAddress buildInetSocketAddress(String addressPort)
	{
		InetSocketAddress inetSocketAddress = null;
		if (addressPort == null) return null;
		String[] arr = addressPort.split(":");
		if (arr.length < 2) return null;
		int port = Integer.parseInt(arr[1]);
		inetSocketAddress = new InetSocketAddress(arr[0], port);
		return inetSocketAddress;
	}
}
