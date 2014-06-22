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
package com.zentral.sembilar.resp.type;

import java.util.ArrayList;
import java.util.List;

public class RespArray extends RespType {
	private int size;
	private List<RespType> element = new ArrayList<RespType>();

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public List<RespType> getElement() {
		return element;
	}

	public void setElement(List<RespType> element) {
		this.element = element;
	}
	
	@Override
	public String toString()
	{
		String str = "";
		for (int i=0;i<getElement().size();i++)
		{
			RespType t = getElement().get(i);
			str = str + "|" + t.toString();
		}		
		return str;
	}

}
