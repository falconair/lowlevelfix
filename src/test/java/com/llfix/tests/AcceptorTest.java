package com.llfix.tests;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.junit.Test;

import com.llfix.DefaultLogonManager;
import com.llfix.FIXAcceptorPipelineFactory;
import com.llfix.util.FieldAndRequirement;

public class AcceptorTest {
	
	private static long execID = 1;

	@Test
	public void acceptorTest() throws IOException{
		final ServerChannelFactory cf = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

		final List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
		final List<FieldAndRequirement> trailerFields = new ArrayList<FieldAndRequirement>();

		final ServerBootstrap server = new ServerBootstrap(cf);
		server.setPipelineFactory(new FIXAcceptorPipelineFactory(headerFields, trailerFields));
		
		server.bind(new InetSocketAddress(9878));

		while(true) System.in.read();
	}
	
	public void simpleFillTest() throws IOException{
		final ServerChannelFactory cf = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

		final List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
		final List<FieldAndRequirement> trailerFields = new ArrayList<FieldAndRequirement>();

		final ServerBootstrap server = new ServerBootstrap(cf);
		server.setPipelineFactory(new FIXAcceptorPipelineFactory(
				headerFields, 
				trailerFields,
				new DefaultLogonManager(),
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
                	
                }));
		
		server.bind(new InetSocketAddress(9878));

		while(true) System.in.read();
	}
	
	public static void main(String ... args) throws IOException{
		new AcceptorTest().acceptorTest();
	}
}
