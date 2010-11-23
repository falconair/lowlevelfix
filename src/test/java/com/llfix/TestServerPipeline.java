package com.llfix;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.local.DefaultLocalClientChannelFactory;
import org.jboss.netty.channel.local.DefaultLocalServerChannelFactory;
import org.jboss.netty.channel.local.LocalAddress;
import org.jboss.netty.channel.local.LocalClientChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import com.llfix.handlers.FIXMessageDecoder;
import com.llfix.util.FieldAndRequirement;

public class TestServerPipeline<E> {

	private final Channel clientChannel;
	private final Channel serverChannel;
	private final Queue<E> output = new ConcurrentLinkedQueue<E>();

	public TestServerPipeline(final LocalAddress localAddress, final ChannelHandler ... handlers){

		//Server
		final ServerChannelFactory serverFactory = new DefaultLocalServerChannelFactory();

		final ServerBootstrap server = new ServerBootstrap(serverFactory);
		server.setPipelineFactory(new ChannelPipelineFactory() {

			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipe = org.jboss.netty.channel.Channels.pipeline(handlers);
				return pipe;
			}
		});

		serverChannel = server.bind(localAddress);
		
		//Client
		final LocalClientChannelFactory clientFactory = new DefaultLocalClientChannelFactory();
		
		final ClientBootstrap client = new ClientBootstrap(clientFactory);
		client.setPipelineFactory(new ChannelPipelineFactory() {
			
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipe = org.jboss.netty.channel.Channels.pipeline(
						new StringDecoder(),
						new StringEncoder(),
						new FIXMessageDecoder(new ArrayList<FieldAndRequirement>(), new ArrayList<FieldAndRequirement>()),
						new SimpleChannelHandler(){

							@Override
							public void messageReceived( ChannelHandlerContext ctx, MessageEvent e) throws Exception {
								output.add((E) e.getMessage());
							}});
				return pipe;
			}
		});
		
		clientChannel = client.connect(localAddress).getChannel();
	}
	
	public void offer(String msg){
		clientChannel.write(msg);
	}
	
	public E peek(){
		return output.peek();
	}
	
	public E poll(){
		return output.poll();
	}

}
