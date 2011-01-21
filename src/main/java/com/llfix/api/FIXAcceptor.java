package com.llfix.api;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ObjectArrays;
import com.llfix.FIXAcceptorPipelineFactory;
import com.llfix.ILogonManager;
import com.llfix.IMessageCallback;
import com.llfix.IQueueFactory;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.util.DefaultLogonManager;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.MemoryQueueFactory;

final public class FIXAcceptor {
	
	final static Logger logger = LoggerFactory.getLogger(FIXSessionProcessor.class);

	private final int remotePort;
	
	private final boolean isDebugOn;
	
	private final List<FieldAndRequirement> headerFields;
	private final List<FieldAndRequirement> trailerFields;
	
	private final ILogonManager logonManager;
	
	private final Map<String,Channel> sessions;
	private final IQueueFactory<String> queueFactory;
	
	private final List<IMessageCallback> listeners = new ArrayList<IMessageCallback>();

	private final ChannelHandler[] channelHandlers;
	
	private FIXAcceptor(int remotePort, boolean isDebugOn,
			List<FieldAndRequirement> headerFields,
			List<FieldAndRequirement> trailerFields,
			Map<String,Channel> sessions,
			IQueueFactory<String> queueFactory,
			ILogonManager logonManager,
			ChannelHandler ... channelHandlers) {
		super();
		this.remotePort = remotePort;
		this.isDebugOn = isDebugOn;
		this.headerFields = headerFields;
		this.trailerFields = trailerFields;
		this.sessions = sessions;
		this.queueFactory = queueFactory;
		this.logonManager = logonManager;
		this.channelHandlers = channelHandlers;
	}
	
	public void startListening(){
		final ServerSocketChannelFactory cf = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		final ServerBootstrap server = new ServerBootstrap(cf);
		server.setPipelineFactory(new FIXAcceptorPipelineFactory(
				headerFields, 
				trailerFields,
				isDebugOn,
				logonManager,
				sessions,
				queueFactory,
				ObjectArrays.concat(channelHandlers, new MessageBroadcaster(listeners))));
		server.bind(new InetSocketAddress("localhost", remotePort));
	
	}
	
	public void onMsg(IMessageCallback callback){
		listeners.add(callback);
	}
	
	public void sendMsg(String senderCompID, Map<String,String> msg){
		final Channel channel = sessions.get(senderCompID);
		if(channel!=null) channel.write(msg);
	}
	
	public void killConnection(String senderCompID, String reason){
		logger.warn("Attempting to force close session for sender "+senderCompID+" for reason: "+reason);

		final Channel channel = sessions.get(senderCompID);
		if(channel!=null) channel.close();
	}
	
	public void logOff(String senderCompID, String reason){
		final Channel channel = sessions.get(senderCompID);
		
		final Map<String,String> logoff = new LinkedHashMap<String, String>();
		logoff.put("35", "5");
		if(channel!=null) channel.write(logoff);
	}


	public int getRemotePort() {
		return remotePort;
	}
	
	public Iterator<String> getAllMsgsFromStorage(String targetCompID) throws Exception{
		return queueFactory.getQueue(targetCompID).iterator();
	}



	public List<FieldAndRequirement> getHeaderFields() {
		return headerFields;
	}



	public List<FieldAndRequirement> getTrailerFields() {
		return trailerFields;
	}


	public static Builder Builder(String compID, int remotePort){
		return new Builder(compID, remotePort);
	}

	private static final class MessageBroadcaster implements ChannelUpstreamHandler {
		private final List<IMessageCallback> listeners;

		public MessageBroadcaster(List<IMessageCallback> listeners) {
			this.listeners = listeners;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if(e instanceof MessageEvent){
				for(IMessageCallback cb : listeners){
					cb.onMsg((Map<String,String>) ((MessageEvent)e).getMessage());
				}
			}
			else if(e instanceof ExceptionEvent){
				for(IMessageCallback cb : listeners){
					cb.onException(((ExceptionEvent)e).getCause());
				}
			}
			
		}
	}

	public static class Builder{
		
		private final int remotePort;
		private final String compID;
		
		private boolean isDebugOn = false;
		
		private List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
		private List<FieldAndRequirement> trailerFields = new ArrayList<FieldAndRequirement>();
		
		private ILogonManager logonManager = null;
		
		private Map<String,Channel> sessions= new ConcurrentHashMap<String, Channel>();
		private IQueueFactory<String> queueFactory = new MemoryQueueFactory<String>();
		
		private ChannelHandler[] channelHandlers = new ChannelHandler[0];

		
		public Builder(final String compID, int remotePort) {
			super();
			this.compID = compID;
			this.remotePort = remotePort;
			this.logonManager = new DefaultLogonManager(compID);
		}
		
		public Builder withSessionStoreFactory(Map<String,Channel> sessions){
			this.sessions = sessions;
			return this;
		}
		
		public Builder withAdditionalHandlers(ChannelHandler ... handlers){
			channelHandlers = handlers;
			return this;
		}
		
		public Builder withMsgStoreFactory(IQueueFactory<String> queueFactory){
			this.queueFactory = queueFactory;
			return this;
		}
		
		public Builder withDebugStatus(boolean isOn){
			this.isDebugOn = isOn;
			return this;
		}
				
		public Builder withFieldRequirements(List<FieldAndRequirement> headerFields, List<FieldAndRequirement> trailerFields){
			this.headerFields = headerFields;
			this.trailerFields = trailerFields;
			return this;
		}
		
		public Builder withLogonManager(ILogonManager logonManager){
			this.logonManager = logonManager;
			return this;
		}
		
		public FIXAcceptor build(){
			return new FIXAcceptor(
					remotePort,
					isDebugOn,
					headerFields, 
					trailerFields,
					sessions,
					queueFactory,
					logonManager,
					channelHandlers);
		}
		
		
	}
}
