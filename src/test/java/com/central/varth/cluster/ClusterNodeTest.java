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

package com.central.varth.cluster;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.central.sembilar.resp.cluster.ClusterNode;
import com.central.sembilar.resp.parser.ClusterNodeParser;
import com.central.sembilar.resp.parser.impl.RedisClusterNodeParser;

public class ClusterNodeTest {

	@Test
	public void parsingTest()
	{
		String raw = "1350e2bb7b20f160e54629958b4dabb772c661b0 127.0.0.1:7002 master - 0 1402691777661 22 connected 0-565 5962-10922 11422-11855\n" + 
				"d8806a2ec150cc0153b4ea16d837e7958f81b4cb 127.0.0.1:7006 slave 35919c478bfd35677d33902badc2508dc51776d0 0 1402691777661 21 connected\n" + 
				"ddebc4aeb5a96b5ceffd0d64c571c3885c2e7ded :0 myself,slave 7ce9cdc2c8dec44a0f09096af6f8f9865257e724 0 0 19 connected\n" + 
				"35919c478bfd35677d33902badc2508dc51776d0 127.0.0.1:7003 master - 0 1402691778165 21 connected 11856-16383\n" + 
				"6ab17726d88c1fdf8693b22b25aef8ba0e806efa 127.0.0.1:7005 slave 1350e2bb7b20f160e54629958b4dabb772c661b0 0 1402691777158 22 connected\n" + 
				"7ce9cdc2c8dec44a0f09096af6f8f9865257e724 127.0.0.1:7001 master - 0 1402691778670 23 connected 566-5961 10923-11421\n" + 
				"";
		ClusterNodeParser parser = new RedisClusterNodeParser();
		List<ClusterNode> nodes = parser.parse(raw);
		Assert.assertNotNull(nodes);
		Assert.assertEquals(6, nodes.size());
		
	}
}
