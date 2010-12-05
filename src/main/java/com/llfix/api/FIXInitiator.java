package com.llfix.api;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.llfix.FIXInitiatorPipelineFactory;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.IMessageCallback;
import com.llfix.util.IQueueFactory;
import com.llfix.util.SimpleQueueFactory;

final public class FIXInitiator {

	private final String version;
	private final String senderCompID;
	private final String targetCompID;
	
	private final String remoteAddress;
	private final int remotePort;
	
	private final int heartBeat;
	
	private final boolean isDebugOn;
	
	private final List<FieldAndRequirement> headerFields;
	private final List<FieldAndRequirement> trailerFields;

	private final Map<String,Channel> sessions;
	private final IQueueFactory<String> queueFactory;
	
	private final List<IMessageCallback> listeners = new ArrayList<IMessageCallback>();

	private Channel channel;

	
	private FIXInitiator(String version, String senderCompID, String targetCompID, 
			String remoteAddress, int remotePort, int heartBeat, boolean isDebugOn,
			List<FieldAndRequirement> headerFields,
			List<FieldAndRequirement> trailerFields,
			Map<String,Channel> sessions,
			IQueueFactory<String> queueFactory) {
		super();
		this.version = version;
		this.senderCompID = senderCompID;
		this.targetCompID = targetCompID;
		this.remoteAddress = remoteAddress;
		this.remotePort = remotePort;
		this.heartBeat = heartBeat;
		this.isDebugOn = isDebugOn;
		this.headerFields = headerFields;
		this.trailerFields = trailerFields;
		this.sessions = sessions;
		this.queueFactory = queueFactory;
	}
	
	public void logOn(final Map<String,String> logon){
		final ClientSocketChannelFactory cf = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		final ClientBootstrap client = new ClientBootstrap(cf);
		client.setPipelineFactory(new FIXInitiatorPipelineFactory(
				headerFields, 
				trailerFields,
				sessions,
				queueFactory,
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
				},isDebugOn));
		final ChannelFuture channelFut = client.connect(new InetSocketAddress(remoteAddress, remotePort));
		
		channelFut.addListener(new ChannelFutureListener() {
			

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				System.out.println("Connection status:"+future);
				channel = future.getChannel();
				
				

				ChannelFuture x = channel.write(logon);
				x.addListener(new ChannelFutureListener() {
					
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						System.out.println("Write future:"+future);
					}
				});
			}
		});
	}
	
	public void logOn(){
		final Map<String,String> logon = new LinkedHashMap<String, String>();
		logon.put("8", version);
		logon.put("56", targetCompID);
		logon.put("49", senderCompID);

		logon.put("35", "A");
		logon.put("98", "0");
		logon.put("108", Integer.toString(heartBeat));
		logOn(logon);
	
	}
	
	public void logOff(){
		final Map<String,String> logoff = new LinkedHashMap<String, String>();
		logoff.put("8", version);
		logoff.put("56", targetCompID);
		logoff.put("49", senderCompID);

		logoff.put("35", "5");
		logOff(logoff); 
	}
	
	public void logOff(Map<String,String> msg){
		channel.write(msg);
	}
	
	public void onMsg(IMessageCallback callback){
		listeners.add(callback);
	}
	
	public void sendMsg(Map<String,String> msg){
		channel.write(msg);
	}



	public String getVersion() {
		return version;
	}



	public String getSenderCompID() {
		return senderCompID;
	}



	public String getTargetCompID() {
		return targetCompID;
	}



	public String getRemoteAddress() {
		return remoteAddress;
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



	public int getHeartBeat() {
		return heartBeat;
	}


	public static Builder Builder(String version, String senderCompID,String targetCompID, String remoteAddress, int remotePort){
		return new Builder(version, senderCompID, targetCompID, remoteAddress, remotePort);
	}

	public static class Builder{
		private final String version;
		private final String senderCompID;
		private final String targetCompID;
		
		private final String remoteAddress;
		private final int remotePort;
		
		private int heartBeat = 30;
		private boolean isDebugOn = false;
		
		private List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
		private List<FieldAndRequirement> trailerFields = new ArrayList<FieldAndRequirement>();
		
		private Map<String,Channel> sessions = new ConcurrentHashMap<String, Channel>();
		private IQueueFactory<String> queueFactory = new SimpleQueueFactory<String>();
		
		public Builder(String version, String senderCompID,String targetCompID, String remoteAddress, int remotePort) {
			super();
			this.version = version;
			this.senderCompID = senderCompID;
			this.targetCompID = targetCompID;
			this.remoteAddress = remoteAddress;
			this.remotePort = remotePort;
		}
		
		public Builder withSessionStoreFactory(Map<String,Channel> sessions){
			this.sessions = sessions;
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
		
		public Builder withHeartBeatSeconds(int heartBeat){
			this.heartBeat = heartBeat;
			return this;
		}
		
		public Builder withFieldRequirements(List<FieldAndRequirement> headerFields, List<FieldAndRequirement> trailerFields){
			this.headerFields = headerFields;
			this.trailerFields = trailerFields;
			return this;
		}
		
		public FIXInitiator build(){
			return new FIXInitiator(
					version, 
					senderCompID, 
					targetCompID, 
					remoteAddress, 
					remotePort,
					heartBeat,
					isDebugOn,
					headerFields, 
					trailerFields,
					sessions,
					queueFactory);
		}
		
		
	}
}
