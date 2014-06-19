package com.central.varth.resp;

import java.net.InetSocketAddress;

public class MovedException extends RespException
{

	private static final long serialVersionUID = 4960917428625785631L;

	public int slot;
	public InetSocketAddress address;
	
	public MovedException(String message)
	{
		super(message);
	}

}
