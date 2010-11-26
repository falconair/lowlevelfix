package com.llfix.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.junit.Test;

import com.llfix.DefaultLogonManager;
import com.llfix.ILogonManager;
import com.llfix.TestServerPipeline;
import com.llfix.handlers.FIXFrameDecoder;
import com.llfix.handlers.FIXMessageDecoder;
import com.llfix.handlers.FIXMessageEncoder;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.handlers.LogHandler;
import com.llfix.util.FieldAndRequirement;


public class FIXStandardTestCases {
	
	final List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
    final List<FieldAndRequirement> trailerFields= new ArrayList<FieldAndRequirement>();
    final ILogonManager logonManager = new DefaultLogonManager();
	
	private static StringDecoder STRINGDECODER = new StringDecoder();
    private static StringEncoder STRINGENCODER = new StringEncoder();
    private static LogHandler LOGHANDLER = new LogHandler();

	/**
	 * Receive Logon message, then receive another logon message
	 * @throws IOException 
	 */
	@Test public void S1ValidAndDuplicateLogon() throws IOException{
	    final Set<String> sessions = new HashSet<String>();

		final TestServerPipeline<Map<String,String>> server = new TestServerPipeline<Map<String,String>>(new LocalAddress(1234),
				new IdleStateHandler(new org.jboss.netty.util.HashedWheelTimer(), 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                LOGHANDLER,
                new FIXMessageEncoder(headerFields, trailerFields),
                new FIXMessageDecoder(),
                new FIXSessionProcessor(false, headerFields, trailerFields, logonManager, sessions));
		
		String logon = "8=FIX.4.29=6735=A34=149=CLIENT52=20101117-03:40:57.70556=SERVER98=0108=3010=147";
		server.offer(logon);
		Assert.assertEquals("A", server.poll().get("35"));				
		
		server.offer(logon);
		Assert.assertEquals(null, server.poll());				
	}

	/**
	 * Receive the first message which is not a logon
	 * @throws IOException 
	 */
	@Test public void S1NonLogon() throws IOException{
	    final Set<String> sessions = new HashSet<String>();

		final TestServerPipeline<Map<String,String>> server = new TestServerPipeline<Map<String,String>>(new LocalAddress(1234),
				new IdleStateHandler(new org.jboss.netty.util.HashedWheelTimer(), 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                LOGHANDLER,
                new FIXMessageEncoder(headerFields, trailerFields),
                new FIXMessageDecoder(),
                new FIXSessionProcessor(false, headerFields, trailerFields, logonManager, sessions));
		
		String logon = "8=FIX.4.29=5935=034=349=HTX_DC52=20080912-13:24:19.56356=MPNLOMS4NJ10=174";
		server.offer(logon);
		Assert.assertEquals(null, server.poll());				
	}
	
	/**
	 * Logon with wrong seqnum
	 * @throws IOException 
	 */
	@Test public void S1LogonWithWrongSeqNum() throws IOException{
	    final Set<String> sessions = new HashSet<String>();

		final TestServerPipeline<Map<String,String>> server = new TestServerPipeline<Map<String,String>>(new LocalAddress(1234),
				new IdleStateHandler(new org.jboss.netty.util.HashedWheelTimer(), 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                LOGHANDLER,
                new FIXMessageEncoder(headerFields, trailerFields),
                new FIXMessageDecoder(),
                new FIXSessionProcessor(false, headerFields, trailerFields, logonManager, sessions));
		
		String logon = "8=FIX.4.29=7135=A34=549=HTX_DC52=20080912-13:23:19.55556=MPNLOMS4NJ98=0108=3010=212";
		server.offer(logon);
		//server.poll();//first logon ack, then resend?
		Assert.assertEquals("2", server.poll().get("35"));				
	}

}
