package com.central.sembilar.resp;

public class StringDeserializer implements RespDeserializer<String> {

	@Override
	public String deserialize(String raw) {
		return raw;
	}

}
