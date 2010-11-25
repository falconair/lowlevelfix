package com.llfix.tests;

import static java.lang.System.out;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.llfix.api.FIXInitiator;
import com.llfix.util.IMessageCallback;

public class FIXInitiatorTests {

	@Test 
	public void connectionTest() throws IOException{
		FIXInitiator client = FIXInitiator.Builder("FIX.4.2", "CLIENT", "SERVER", "localhost", 5555).withDebugStatus(true).build();

		client.onMsg(new IMessageCallback() {
			
			@Override
			public void onMsg(Map<String, String> msg) {
				out.println(msg);
			}
			
			@Override
			public void onException(Throwable t) {
				t.printStackTrace();
			}
		});
		
		client.logOn();
		
		System.in.read();
	}
}
