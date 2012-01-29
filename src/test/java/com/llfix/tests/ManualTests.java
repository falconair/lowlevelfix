package com.llfix.tests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.junit.Test;

import com.llfix.api.FIXAcceptor;
import com.llfix.api.FIXInitiator;
import com.llfix.util.DiskQueueFactory;

public class ManualTests {
	
	@Test
	public void testClient() throws UnknownHostException, IOException{
		FIXClient client = FIXClient.createNewSession("sender","target").connect("localhost",1234);

		Map<String,String> msg = new HashMap<String,String>();
		msg.put("35", "A");

		client.send(msg);
	}

	@Test
	public void testFIXEncoding(){
		FIXClient client = new FIXClient("sender","target","FIX.4.2")
			.autoPopulateBodyLength(false)
			.autoPopulateChecksum(false)
			.autoPopulateSenderCompID(false)
			.autoPopulateTargetCompID(false)
			.autoPopulateVersion(false)
			.autoPopulateTimeStamp(false)
			.autoPopulateMsgSeqNum(false);

		Map<String,String> msg = new HashMap<String,String>();
		msg.put("35", "A");

		DateTime dummyTime = new DateTime(2012,1,15,6,44,4,886);

		String fix = client.encodeToFIXWithTime(msg, dummyTime);
		assertEquals("35=A",fix);

		client.autoPopulateSenderCompID(true);
		fix = client.encodeToFIXWithTime(msg, dummyTime);
		assertEquals("35=A49=sender",fix);

		client.autoPopulateTargetCompID(true);
		fix = client.encodeToFIXWithTime(msg, dummyTime);
		assertEquals("35=A49=sender56=target",fix);

		client.autoPopulateVersion(true);
		fix = client.encodeToFIXWithTime(msg, dummyTime);
		assertEquals("8=FIX.4.235=A49=sender56=target",fix);

		client.autoPopulateTimeStamp(true);
		fix = client.encodeToFIXWithTime(msg, dummyTime);
		assertEquals("8=FIX.4.235=A49=sender56=target52=20120115-11:44:04.886",fix);

		client.autoPopulateMsgSeqNum(true);
		fix = client.encodeToFIXWithTime(msg, dummyTime);
		assertEquals("8=FIX.4.235=A49=sender56=target52=20120115-11:44:04.88634=1",fix);
	}
	
	//Old tests

	@Test
	public void initiatorTest() throws IOException{
		final FIXInitiator fix = FIXInitiator.Builder("FIX.4.2", "CLIENT", "SERVER")
			.withHeartBeats(true)
			.with...
			.withHeartBeatSeconds(10)
			.withDebugStatus(true)
			.withMsgStoreFactory(new DiskQueueFactory("data"))
			.build();
		fix.connectAndLogOn("localhost", 5555);
		
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
		final FIXAcceptor fix = FIXAcceptor.Builder("TARGET")
			.withDebugStatus(true)
			.withMsgStoreFactory(new DiskQueueFactory("data")).build();
		fix.startListening(5555);

		Executors.newScheduledThreadPool(2).schedule(new Runnable() {
			
			@Override
			public void run() {
				fix.logOff("SENDER","TIMETOGO");
			}
		}, 30L, TimeUnit.SECONDS);

		System.in.read();
	}
}
