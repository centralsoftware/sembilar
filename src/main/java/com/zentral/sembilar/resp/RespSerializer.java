package com.zentral.sembilar.resp;

import java.io.Serializable;

public interface RespSerializer<T extends Serializable> {

	public String serialize(T object);
	
}

