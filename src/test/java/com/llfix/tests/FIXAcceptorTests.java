package com.llfix.tests;

import java.io.IOException;

import org.junit.Test;

import com.llfix.api.FIXAcceptor;

public class FIXAcceptorTests {

	@Test
	public void connectionTest() throws IOException{
		FIXAcceptor server = FIXAcceptor.Builder(5555).withDebugStatus(true).build();
		server.startListening();
		
		System.in.read();
	}
}
