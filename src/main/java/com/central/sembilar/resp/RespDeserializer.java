package com.central.sembilar.resp;

import java.io.Serializable;

public interface RespDeserializer<T extends Serializable> {

	public T deserialize(String raw);
}
