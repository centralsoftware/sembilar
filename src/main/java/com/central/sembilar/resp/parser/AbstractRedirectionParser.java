package com.central.sembilar.resp.parser;

import java.net.InetSocketAddress;

import com.central.sembilar.resp.ProtocolConstant;

public abstract class AbstractRedirectionParser<T> implements RedirectionParser<T>
{

	/**
	 * get inetsocketaddress from address:port
	 * @param addressPort
	 * @return
	 */
	protected InetSocketAddress getAddress(String addressPort)
	{
		String[] addressPortArr = addressPort.split(ProtocolConstant.COLON);
		String address = addressPortArr[0];
		int port = Integer.parseInt(addressPortArr[1]);
		return new InetSocketAddress(address, port);
	}	
}
