package com.central.sembilar.resp.parser.impl;

import com.central.sembilar.resp.parser.AbstractRedirectionParser;
import com.central.varth.resp.MovedInfo;
import com.central.varth.resp.ProtocolConstant;

public class RedisMovedRedirectionParser extends AbstractRedirectionParser<MovedInfo> {

	/**
	 * raw format:
	 * -MOVED slot address:port
	 */
	@Override
	public MovedInfo parse(String raw) {
		String[] cols = raw.split(ProtocolConstant.SPACE);
		MovedInfo info = new MovedInfo(raw);
		info.setSlot(Integer.parseInt(cols[1]));	
		info.setAddress(getAddress(cols[2]));
		return info;
	}
	
}
