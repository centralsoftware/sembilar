package com.central.sembilar.resp.parser.impl;

import com.central.sembilar.resp.AskInfo;
import com.central.sembilar.resp.ProtocolConstant;
import com.central.sembilar.resp.parser.AbstractRedirectionParser;

public class RedisAskRedirectionParser extends AbstractRedirectionParser<AskInfo>
{

	/**
	 * raw format:
	 * -ASK slot address:port
	 */	
	@Override
	public AskInfo parse(String raw) {
		String[] cols = raw.split(ProtocolConstant.SPACE);		
		AskInfo info = new AskInfo(raw);
		info.setSlot(Integer.parseInt(cols[1]));	
		info.setAddress(getAddress(cols[2]));
		return info;
	}
	
}
