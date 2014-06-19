package com.central.varth.resp;

import java.net.InetSocketAddress;

public class MovedInfo {
	private String raw;
	private int slot;
	private InetSocketAddress address;
	
	public MovedInfo(String raw)
	{
		this.raw = raw;
	}

	public int getSlot() {
		return slot;
	}

	public void setSlot(int slot) {
		this.slot = slot;
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	public void setAddress(InetSocketAddress address) {
		this.address = address;
	}

	public String getRaw() {
		return raw;
	}
	
	
}
