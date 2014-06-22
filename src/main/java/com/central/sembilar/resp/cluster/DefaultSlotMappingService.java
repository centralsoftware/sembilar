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

package com.central.sembilar.resp.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.central.sembilar.resp.ProtocolConstant;
import com.central.sembilar.resp.connection.RespClient;

public class DefaultSlotMappingService implements SlotMappingService {

	
	@Override
	public Map<Integer, RespClient> buildMap(List<RespClient> clients) {
		Map<Integer, RespClient> map = new HashMap<Integer, RespClient>();
		for (int i=0;i<ProtocolConstant.SLOT_MAX_SIZE;i++)
		{
			RespClient client = findClient(i, clients);
			map.put(i, client);
		}
		return map;
	}
	
	protected RespClient findClient(int slot, List<RespClient> clients)
	{
		for (RespClient client:clients)
		{
			if (client != null && client.hasSlot(slot))
			{
				return client;
			}
		}
		return null;
	}

	@Override
	public Map<Integer, RespClient> getSlotMap() {
		// TODO Auto-generated method stub
		return null;
	}

}
