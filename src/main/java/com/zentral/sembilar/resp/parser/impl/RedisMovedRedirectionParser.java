package com.zentral.sembilar.resp.parser.impl;

import com.zentral.sembilar.resp.MovedInfo;
import com.zentral.sembilar.resp.ProtocolConstant;
import com.zentral.sembilar.resp.parser.AbstractRedirectionParser;

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
