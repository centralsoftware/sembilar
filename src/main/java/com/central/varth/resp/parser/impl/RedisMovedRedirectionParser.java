package com.central.varth.resp.parser.impl;

import java.net.InetSocketAddress;

import com.central.varth.resp.MovedInfo;
import com.central.varth.resp.ProtocolConstant;
import com.central.varth.resp.parser.MovedRedirectionParser;

public class RedisMovedRedirectionParser implements MovedRedirectionParser {

	/**
	 * raw format:
	 * -MOVED slot address:port
	 */
	@Override
	public MovedInfo parse(String raw) {
		MovedInfo info = new MovedInfo(raw);
		info.setSlot(getSlot(raw));	
		info.setAddress(getAddress(raw));
		return null;
	}
	
	private int getSlot(String raw)
	{
		String[] cols = raw.split(ProtocolConstant.SPACE);
		int slot = Integer.parseInt(cols[1]);
		return slot;
	}
	
	private InetSocketAddress getAddress(String raw)
	{
		String[] cols = raw.split(ProtocolConstant.SPACE);
		String addressPort = cols[2];
		String[] addressPortArr = addressPort.split(ProtocolConstant.SEMICOLON);
		String address = addressPortArr[0];
		int port = Integer.parseInt(addressPortArr[1]);
		return new InetSocketAddress(address, port);
	}

}
