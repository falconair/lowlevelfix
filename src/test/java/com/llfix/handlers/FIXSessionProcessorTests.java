package com.llfix.handlers;

import static java.lang.System.out;

import java.net.InetAddress;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.*;

import org.junit.Test;

import com.llfix.ILogonManager;
import com.llfix.IMessageCallback;
import com.llfix.IQueueFactory;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.util.DefaultLogonManager;
import com.llfix.util.MemoryQueueFactory;

public class FIXSessionProcessorTests {
	
	@Test public void frameDecoderZeroConsumedTest() throws ParseException, Exception{
		final AtomicBoolean callbackProcessed = new AtomicBoolean(false);
		
		String fixstr1  = "8=FIX.4.19=6135=A34=149=EXEC52=20121105-23:24:0656=BANZAI98=0108=";
		
		final int consumed = FIXSessionProcessor.frameDecodeFIXMsg(fixstr1, new IMessageCallback<String>() {
			
			@Override
			public void onMsg(String msg) {
				callbackProcessed.set(true);
				out.println(msg);
			}
			
			@Override
			public void onException(Throwable t) {
				t.printStackTrace();
				assertFalse(t.getMessage(), true);
				
			}
		});
		
		assertEquals(0, consumed);
		assertFalse("Callback should never be never called", callbackProcessed.get());
	}
	
	@Test public void frameDecoderSingleMsgTest() throws ParseException, Exception{
		final AtomicInteger callbackProcessed = new AtomicInteger(0);
		
		String fixstr1  = "8=FIX.4.19=6135=A34=149=EXEC52=20121105-23:24:0656=BANZAI98=0108=3010=0038=FIX.4.19=6135=A34=149=BANZA";
		String fixstr2  = "I52=20121105-23:24:0656=EXEC98=0108=3010=0038=FIX.4.19=4935=034=249=BANZAI52=20121105-23:24:3756=EXE";

		StringBuilder fix = new StringBuilder();
		fix.append(fixstr1).append(fixstr2);
		
		final int consumed = FIXSessionProcessor.frameDecodeFIXMsg(fix.toString(), new IMessageCallback<String>() {
			
			@Override
			public void onMsg(String msg) {
				callbackProcessed.incrementAndGet();
				if(callbackProcessed.get()==1)assertEquals("8=FIX.4.19=6135=A34=149=EXEC52=20121105-23:24:0656=BANZAI98=0108=3010=003", msg);
				if(callbackProcessed.get()==2)assertEquals("8=FIX.4.19=6135=A34=149=BANZAI52=20121105-23:24:0656=EXEC98=0108=3010=003", msg);
				out.println(msg);
			}
			
			@Override
			public void onException(Throwable t) {
				t.printStackTrace();
				assertFalse(t.getMessage(), true);
				
			}
		});
		
		assertEquals(166, consumed);
		assertEquals("Callback should have been called 2 times",callbackProcessed.get() , 2);
	}

	@Test
	public void basicTest() throws Exception{
		boolean isInitiator = true;
		ILogonManager logonManager = new DefaultLogonManager("BANZAI");
		Set<String> sessions = new HashSet<String>();
		InetAddress remoteAddress = InetAddress.getLocalHost();
		IQueueFactory<String> qFactory = new MemoryQueueFactory<String>();

		IMessageCallback<Map<String, String>> outgoingCallback = new IMessageCallback<Map<String, String>>() {
			
			@Override
			public void onMsg(Map<String, String> msg) {
				out.println("Outgoing: "+msg);
			}
			
			@Override
			public void onException(Throwable t) {
				out.println("Outgoing: "+t);
			}
		};
		
		IMessageCallback<Map<String, String>> incomingCallback = new IMessageCallback<Map<String, String>>() {
			
			@Override
			public void onMsg(Map<String, String> msg) {				
				out.println("Incoming: "+msg);
			}
			
			@Override
			public void onException(Throwable t) {
				out.println("Incoming: "+t);
			}
		};

		FIXSessionProcessor fix = new FIXSessionProcessor(isInitiator, logonManager, sessions, qFactory, outgoingCallback, incomingCallback, remoteAddress);

		String fixstr1  = "8=FIX.4.19=6135=A34=149=EXEC52=20121105-23:24:0656=BANZAI98=0108=3010=0038=FIX.4.19=6135=A34=149=BANZA";
		String fixstr2  = "I52=20121105-23:24:0656=EXEC98=0108=3010=0038=FIX.4.19=4935=034=249=BANZAI52=20121105-23:24:3756=EXE";
		String fixstr3  ="C10=2288=FIX.4.19=4935=034=249=EXEC52=20121105-23:24:3756=BANZAI10=2288=FIX.4.19=10335=D34=349=BANZAI52=2";
		String fixstr4  ="0121105-23:24:4256=EXEC11=135215788257721=138=1000040=154=155=MSFT59=010=0628=FIX.4.19=13935=834=349=EXEC";
		String fixstr5  ="52=20121105-23:24:4256=BANZAI6=011=135215788257714=017=120=031=032=037=138=1000039=054=155=MSFT150=2151=0";
		String fixstr6  ="10=0598=FIX.4.19=15335=834=449=EXEC52=20121105-23:24:4256=BANZAI6=12.311=135215788257714=1000017=220=031=12.33" ;
		String fixstr7  ="2=1000037=238=1000039=254=155=MSFT150=2151=010=2308=FIX.4.19=10335=D34=449=BANZAI52=20121105-23:24:5556=EXEC11=135215" ;
		String fixstr8  ="789503221=138=1000040=154=155=ORCL59=010=0478=FIX.4.19=13935=834=549=EXEC52=20121105-23:24:5556=BANZAI6=011=135215789" ;
		String fixstr9  ="503214=017=320=031=032=037=338=1000039=054=155=ORCL150=2151=010=0498=FIX.4.19=15335=834=649=EXEC52=20121105-23:24:555" ;
		String fixstr10 ="6=BANZAI6=12.311=135215789503214=1000017=420=031=12.332=1000037=438=1000039=254=155=ORCL150=2151=010=2208=FIX.4.19=1" ;
		String fixstr11 ="0835=D34=549=BANZAI52=20121105-23:25:1256=EXEC11=135215791235721=138=1000040=244=1054=155=SPY59=010=0038=FIX.4.19=138" ;
		String fixstr12 ="35=834=749=EXEC52=20121105-23:25:1256=BANZAI6=011=135215791235714=017=520=031=032=037=538=1000039=054=155=SPY150=215" ;
		String fixstr13 ="1=010=2528=FIX.4.19=10435=F34=649=BANZAI52=20121105-23:25:1656=EXEC11=135215791643738=1000041=135215791235754=155=SPY" ;
		String fixstr14 ="10=1988=FIX.4.19=8235=334=849=EXEC52=20121105-23:25:1656=BANZAI45=658=Unsupported message type10=0008=FIX.4.19=1043" ;
		String fixstr15 ="5=F34=749=BANZAI52=20121105-23:25:2556=EXEC11=135215792530938=1000041=135215791235754=155=SPY10=1978=FIX.4.19=8235=3" ;
		String fixstr16 ="34=949=EXEC52=20121105-23:25:2556=BANZAI45=758=Unsupported message type10=002";
		
		fix.processIncoming(fixstr1);
	}

}
