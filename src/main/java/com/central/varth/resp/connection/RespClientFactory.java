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

package com.central.varth.resp.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import com.central.varth.resp.RespException;
import com.central.varth.resp.cluster.ClusterNode;

public interface RespClientFactory {

	public RespClient getInstanceFromNode(ClusterNode node) throws IOException;
	public RespClient getInstanceFromAddress(InetSocketAddress address) throws IOException;
	public List<RespClient> getInstancesFromAddress(List<InetSocketAddress> addresses) throws IOException;
	public List<RespClient> getInstancesFromNodes(List<ClusterNode> nodes) throws RespException;
}
