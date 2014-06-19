package com.central.varth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Ignore;

public class SocketTest {

	@Ignore
	public void redisSocketTest()
	{
		String hostname = "localhost";
		int port = 7001;
		try (
				Socket redisSocket = new Socket(hostname, port);
				PrintWriter out = new PrintWriter(redisSocket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(redisSocket.getInputStream()));
				) {
			String input = "PING";
			out.println(input);
			String received = in.readLine();
			System.err.println(received);
			Assert.assertNotNull(received);
			Assert.assertEquals("+PONG", received);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
