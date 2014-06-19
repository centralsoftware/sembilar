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
package com.central.varth.resp;

import com.central.varth.resp.type.RespError;

public class RespException extends Exception {

	private static final long serialVersionUID = 4044346985743900590L;

	private RespError error;
	
	public RespException()
	{
		
	}
	
	public RespException(String message)
	{
		super(message);
	}
	
	public RespException(String message, Throwable throwable)
	{
		super(message, throwable);
	}	

	public RespException(String message, RespError error)
	{
		super(message);
		this.error = error;
	}
	
	public RespError getError() {
		return error;
	}

	public void setError(RespError error) {
		this.error = error;
	}
}
