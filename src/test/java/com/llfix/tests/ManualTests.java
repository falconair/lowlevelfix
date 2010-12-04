package com.llfix.tests;

import java.io.IOException;

import org.junit.Test;

import com.llfix.api.FIXAcceptor;
import com.llfix.api.FIXInitiator;
import com.llfix.util.DiskQueueFactory;

public class ManualTests {

	@Test
	public void initiatorTest() throws IOException{
		FIXInitiator fix = FIXInitiator.Builder("FIX.4.2", "CLIENT", "SERVER", "localhost", 5555)
			.withHeartBeatSeconds(10)
			.withMsgStoreFactory(new DiskQueueFactory("data"))
			.build();
		fix.logOn();
		
		System.in.read();
	}
	
	@Test
	public void acceptorTest() throws IOException{
		FIXAcceptor fix = FIXAcceptor.Builder(5555).withMsgStoreFactory(new DiskQueueFactory("data")).build();
		fix.startListening();

		System.in.read();
	}
}
