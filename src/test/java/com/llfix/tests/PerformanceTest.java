package com.llfix.tests;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
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

public class PerformanceTest {
	
	final static List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
    final static List<FieldAndRequirement> trailerFields= new ArrayList<FieldAndRequirement>();
    final static ILogonManager logonManager = new DefaultLogonManager();
    private static long execID = 1;
	
	private static StringDecoder STRINGDECODER = new StringDecoder();
    private static StringEncoder STRINGENCODER = new StringEncoder();
    private static LogHandler LOGHANDLER = new LogHandler();


	@Test public static void fillTest(){
		final Set<String> sessions = new HashSet<String>();

		final TestServerPipeline<Map<String,String>> server = new TestServerPipeline<Map<String,String>>(new LocalAddress(1234),
				new IdleStateHandler(new org.jboss.netty.util.HashedWheelTimer(), 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                //LOGHANDLER,
                new FIXMessageEncoder(headerFields, trailerFields),
                new FIXMessageDecoder(headerFields, trailerFields),
                new FIXSessionProcessor(false, headerFields, trailerFields, logonManager, sessions),
                new SimpleChannelUpstreamHandler(){

			@Override
			public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
				final Map<String,String> fix = (Map<String,String>) e.getMessage();
				
				if(fix.get("35").equals("D")){//New Order
					final String side = fix.get("54");
					final String symbol = fix.get("55");
					final double qty = Double.parseDouble(fix.get("38"));
					final double price = fix.get("44")==null?100:Double.parseDouble(fix.get("44"));

					double leaves = qty;
					double cumqty = 0;
					
					for(int i=1; i<qty; i++){
						cumqty++;
						leaves = qty-cumqty;
						
						final Map<String,String> fill = new LinkedHashMap<String, String>();
						fill.put("35", "8");
						fill.put("37", fix.get("11"));//order id
						fill.put("17", Long.toString(execID++));//ExecID
						fill.put("20", "0");//ExecTranType 0=new, 1=cancel, 2=correct
						fill.put("150", "1");//exectype 2=fill, 1=partial, 0=new
						fill.put("39", "1");//ordstatus 2=fill, 1=partial, 0=new
						fill.put("55", symbol);
						fill.put("54", side);
						fill.put("38", Double.toString(qty-leaves));//filled shares
						fill.put("44", Double.toString(price));
						fill.put("151", Double.toString(leaves));//leaves
						fill.put("14", Double.toString(cumqty));//cumqty
						fill.put("6", Double.toString(price));//avgprice
						

                		Channels.write(ctx, Channels.future(ctx.getChannel()), fill);
						
					}
				}
			}
        	
        });
		
		String logon = "8=FIX.4.29=5935=034=349=HTX_DC52=20080912-13:24:19.56356=MPNLOMS4NJ10=174";
		String order = "8=FIX.4.29=12135=D34=349=HTX_DC52=20080912-13:24:19.56356=MPNLOMS4NJ11=121=154=155=MSFT38=1000040=160=20080912-13:24:19.56310=141";

		server.offer(logon);
		server.offer(order);

	}
}
