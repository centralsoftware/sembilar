package com.zentral.sembilar.crc;

import org.junit.Assert;
import org.junit.Test;

import com.zentral.sembilar.resp.crc.CRC16;

public class Crc16 {

	@Test
    public void checksum() { 
        int crc = 0x0000;          // initial value
        int polynomial = 0x1021;   // 0001 0000 0010 0001  (0, 5, 12) 

        String text = "123456789";
        byte[] bytes = text.getBytes();

        for (byte b : bytes) {
            for (int i = 0; i < 8; i++) {
                boolean bit = ((b   >> (7-i) & 1) == 1);
                boolean c15 = ((crc >> 15    & 1) == 1);
                crc <<= 1;
                if (c15 ^ bit) crc ^= polynomial;
             }
        }

        crc &= 0xffff;
        System.err.println("CRC16-CCITT = " + Integer.toHexString(crc));
        
        Assert.assertEquals("31c3", Integer.toHexString(crc));
    }
	
	@Test
	public void checkCRC16()
	{
		String text = "123456789";
		int crc = CRC16.checksum(text);
        Assert.assertEquals("31c3", Integer.toHexString(crc));
	}
}
