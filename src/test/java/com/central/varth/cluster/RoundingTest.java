package com.central.varth.cluster;

import java.math.BigDecimal;

import org.junit.Test;

public class RoundingTest {

	@Test
	public void roundTest()
	{
		int max = 5;
		double rand = max*Math.random();
		BigDecimal index = new BigDecimal(String.valueOf(rand));
		index = index.setScale(0, BigDecimal.ROUND_HALF_UP);
		System.err.println(rand + " " + index.intValue());
	}	
	
}