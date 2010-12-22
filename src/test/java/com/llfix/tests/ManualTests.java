package com.llfix.tests;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.llfix.api.FIXAcceptor;
import com.llfix.api.FIXInitiator;
import com.llfix.util.DiskQueueFactory;

public class ManualTests {

	@Test
	public void initiatorTest() throws IOException{
		final FIXInitiator fix = FIXInitiator.Builder("FIX.4.2", "CLIENT", "SERVER", "localhost", 5555)
			.withHeartBeatSeconds(10)
			.withDebugStatus(true)
			.withMsgStoreFactory(new DiskQueueFactory("data"))
			.build();
		fix.logOn();
		
		Executors.newScheduledThreadPool(2).schedule(new Runnable() {
			
			@Override
			public void run() {
				fix.logOff();
			}
		}, 30L, TimeUnit.SECONDS);
		
		System.in.read();
	}
	
	@Test
	public void acceptorTest() throws IOException{
		final FIXAcceptor fix = FIXAcceptor.Builder(5555)
			.withDebugStatus(true)
			.withMsgStoreFactory(new DiskQueueFactory("data")).build();
		fix.startListening();

		Executors.newScheduledThreadPool(2).schedule(new Runnable() {
			
			@Override
			public void run() {
				fix.logOff("SENDER","TIMETOGO");
			}
		}, 30L, TimeUnit.SECONDS);

		System.in.read();
	}
}
