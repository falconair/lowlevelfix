package com.llfix;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;

import com.llfix.handlers.FIXFrameDecoder;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.handlers.LogHandler;
import com.llfix.util.DefaultLogonManager;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.SimpleQueueFactory;


public class FIXInitiatorPipelineFactory implements ChannelPipelineFactory{

    private final List<FieldAndRequirement> headerFields;
    private final List<FieldAndRequirement> trailerFields;
	private final ChannelUpstreamHandler upstreamHandler;
	private final boolean isDebugOn;
	private final Map<String,Channel> sessions;
	private final IQueueFactory<String> queueFactory;
	
	private final static ILogonManager logOnManager = new DefaultLogonManager();


    public FIXInitiatorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields){
    	this(headerFields,trailerFields, 
    			new ConcurrentHashMap<String, Channel>(), 
    			new SimpleQueueFactory<String>(), 
    			new SimpleChannelUpstreamHandler(), false);
    }

    public FIXInitiatorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields,
            final Map<String,Channel> sessions,
            final IQueueFactory<String> queueFactory,
            final ChannelUpstreamHandler upstreamHandler,
            final boolean isDebugOn){
        this.headerFields = headerFields;
        this.trailerFields = trailerFields;
        this.upstreamHandler = upstreamHandler;
        this.isDebugOn = isDebugOn;
        this.sessions = sessions;
        this.queueFactory = queueFactory;
    }
    
    private static StringDecoder STRINGDECODER = new StringDecoder();
    private static StringEncoder STRINGENCODER = new StringEncoder();
    private static LogHandler LOGHANDLER = new LogHandler();
    private static SimpleChannelUpstreamHandler NOOPHANDLER = new SimpleChannelUpstreamHandler();

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipe = org.jboss.netty.channel.Channels.pipeline(
                new IdleStateHandler(new org.jboss.netty.util.HashedWheelTimer(), 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                isDebugOn ? LOGHANDLER : NOOPHANDLER,
                new FIXSessionProcessor(true,headerFields, trailerFields,logOnManager , sessions, queueFactory),
                upstreamHandler);
        return pipe;
    }
}
