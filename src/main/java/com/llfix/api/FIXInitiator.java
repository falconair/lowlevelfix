package com.llfix.api;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import com.google.common.collect.ObjectArrays;
import com.llfix.IMessageCallback;
import com.llfix.IQueueFactory;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.MemoryQueueFactory;

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

	private final Map<String,Socket> sessions;
	private final IQueueFactory<String> queueFactory;
	
	private final List<IMessageCallback> listeners = new ArrayList<IMessageCallback>();
	private final IMessageCallback outgoingCallback;
	
	private FIXInitiator(String version, String senderCompID, String targetCompID, 
			String remoteAddress, int remotePort, int heartBeat, boolean isDebugOn,
			List<FieldAndRequirement> headerFields,
			List<FieldAndRequirement> trailerFields,
			Map<String,Socket> sessions,
			IQueueFactory<String> queueFactory, 
			IMessageCallback outgoingCallback) {
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
		this.outgoingCallback = outgoingCallback;
	}
	
	public void logOn(final Map<String,String> logon){

		final ClientSocketChannelFactory cf = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
		final ClientBootstrap client = new ClientBootstrap(cf);
		client.setPipelineFactory(new FIXInitiatorPipelineFactory(
				headerFields, 
				trailerFields,
				sessions,
				queueFactory,isDebugOn,
				senderCompID,
				targetCompID,
				outgoingCallback,
				ObjectArrays.concat(channelHandlers, new MessageBroadcaster(listeners))));
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
	
	public Iterator<String> getAllMsgsFromStorage() throws Exception{
		return queueFactory.getQueue(getSenderCompID()+"-"+getTargetCompID()).iterator();
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

	private static final class MessageBroadcaster implements ChannelUpstreamHandler {

		private List<IMessageCallback> listeners;

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
		private final String version;
		private final String senderCompID;
		private final String targetCompID;
		
		private final String remoteAddress;
		private final int remotePort;
		
		private int heartBeat = 30;
		private boolean isDebugOn = false;
		
		private List<FieldAndRequirement> headerFields = new ArrayList<FieldAndRequirement>();
		private List<FieldAndRequirement> trailerFields = new ArrayList<FieldAndRequirement>();
		
		private Map<String,Socket> sessions = new ConcurrentHashMap<String, Socket>();
		private IQueueFactory<String> queueFactory = new MemoryQueueFactory<String>();
		
		private ChannelHandler[] channelHandlers = new ChannelHandler[0];
		private IMessageCallback outgoingCallback;
		
		public Builder(String version, String senderCompID,String targetCompID, String remoteAddress, int remotePort) {
			super();
			this.version = version;
			this.senderCompID = senderCompID;
			this.targetCompID = targetCompID;
			this.remoteAddress = remoteAddress;
			this.remotePort = remotePort;
		}
		
		public Builder withAdditionalHandlers(ChannelHandler ... handlers){
			channelHandlers = handlers;
			return this;
		}
		
		public Builder withSessionStoreFactory(Map<String,Socket> sessions){
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
		
		public Builder withOutgoingMsgCallback(IMessageCallback outgoingCallback){
			this.outgoingCallback = outgoingCallback;
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
					queueFactory,
					outgoingCallback,
					channelHandlers);
		}
		
		
	}
}
