package com.llfix.api;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.llfix.DefaultLogonManager;
import com.llfix.FIXAcceptorPipelineFactory;
import com.llfix.FIXInitiatorPipelineFactory;
import com.llfix.ILogonManager;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.IMessageCallback;

final public class FIXAcceptor {

	private final int remotePort;
	
	private final boolean isDebugOn;
	
	private final List<FieldAndRequirement> headerFields;
	private final List<FieldAndRequirement> trailerFields;
	
	private final ILogonManager logonManager;
	
	private final ConcurrentMap<String,Channel> sessions = new ConcurrentHashMap<String, Channel>();
	
	private final List<IMessageCallback> listeners = new ArrayList<IMessageCallback>();
	
	private FIXAcceptor(int remotePort, boolean isDebugOn,
			List<FieldAndRequirement> headerFields,
			List<FieldAndRequirement> trailerFields,
			ILogonManager logonManager) {
		super();
		this.remotePort = remotePort;
		this.isDebugOn = isDebugOn;
		this.headerFields = headerFields;
		this.trailerFields = trailerFields;
		this.logonManager = logonManager;
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
				new ChannelUpstreamHandler() {
					
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
				}));
		server.bind(new InetSocketAddress("localhost", remotePort));
	
	}
	
	public void onMsg(IMessageCallback callback){
		listeners.add(callback);
	}
	
	public void sendMsg(String senderCompID, Map<String,String> msg){
		final Channel channel = sessions.get(senderCompID);
		if(channel!=null) channel.write(msg);
	}


	public int getRemotePort() {
		return remotePort;
	}



	public List<FieldAndRequirement> getHeaderFields() {
		return headerFields;
	}



	public List<FieldAndRequirement> getTrailerFields() {
		return trailerFields;
	}


	public static Builder Builder(int remotePort){
		return new Builder(remotePort);
	}

	public static class Builder{
		
		private final int remotePort;
		
		private boolean isDebugOn = false;
		
		private List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
		private List<FieldAndRequirement> trailerFields = new ArrayList<FieldAndRequirement>();
		
		private ILogonManager logonManager = new DefaultLogonManager();
		
		public Builder(int remotePort) {
			super();
			this.remotePort = remotePort;
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
					logonManager);
		}
		
		
	}
}
