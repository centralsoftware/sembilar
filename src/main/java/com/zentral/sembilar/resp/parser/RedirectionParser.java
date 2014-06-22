package com.zentral.sembilar.resp.parser;


public interface RedirectionParser<T> {

	public T parse(String raw);	
}
