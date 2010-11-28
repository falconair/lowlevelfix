package com.llfix.handlers;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogHandler extends SimpleChannelHandler {
	
	final static Logger logger = LoggerFactory.getLogger(LogHandler.class);

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.debug("Channel connected: "+e.toString());
		super.channelConnected(ctx, e);
	}

	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.debug("Channel disconnected: "+e.toString());
		super.channelDisconnected(ctx, e);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		logger.debug("Exception: "+e.toString(),e.getCause());
		super.exceptionCaught(ctx, e);
	}

	@Override
	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		logger.debug("Downstream: "+e.toString());
		if(e instanceof MessageEvent){
			logger.info("FIX OUT:"+((MessageEvent)e).getMessage().toString());
		}
		super.handleDownstream(ctx, e);
	}

	@Override
	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
		if(!(e instanceof IdleStateEvent)) logger.debug("Upstream: "+e.toString());
		if(e instanceof MessageEvent){
			logger.info("FIX IN:"+((MessageEvent)e).getMessage().toString());
		}
		super.handleUpstream(ctx, e);
	}

}
