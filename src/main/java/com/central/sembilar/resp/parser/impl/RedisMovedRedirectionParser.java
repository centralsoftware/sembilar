package com.central.sembilar.resp.parser.impl;

import com.central.sembilar.resp.MovedInfo;
import com.central.sembilar.resp.ProtocolConstant;
import com.central.sembilar.resp.parser.AbstractRedirectionParser;

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
