package com.zentral.sembilar.resp;

public class StringSerializer implements RespSerializer<String>{

	@Override
	public String serialize(String object) {
		return object;
	}

}
