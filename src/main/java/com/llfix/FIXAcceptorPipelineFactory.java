package com.llfix;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;

import com.llfix.handlers.FIXFrameDecoder;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.handlers.LogHandler;
import com.llfix.util.DefaultLogonManager;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.MemoryQueueFactory;


public class FIXAcceptorPipelineFactory implements ChannelPipelineFactory{

    private final List<FieldAndRequirement> headerFields;
    private final List<FieldAndRequirement> trailerFields;
    private final ChannelHandler[] upstreamHandlers;
	private final ILogonManager logonManager;
	private final Map<String,Channel> sessions;
	private final IQueueFactory<String> queueFactory;
	private final boolean isDebugOn;

    public FIXAcceptorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields,
            final String compID){
        this(headerFields,trailerFields,true,new DefaultLogonManager(compID),
        		new ConcurrentHashMap<String, Channel>(),
        		new MemoryQueueFactory<String>(),
        		new SimpleChannelUpstreamHandler());
    }
    
    public FIXAcceptorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields,
            final boolean isDebugOn,
            final ILogonManager logonManager,
            final Map<String,Channel> sessions,
            final IQueueFactory<String> queueFactory,
            final ChannelHandler ... upstreamHandler){
        this.headerFields = headerFields;
        this.trailerFields = trailerFields;
        this.logonManager = logonManager;
        this.upstreamHandlers=upstreamHandler;
        this.isDebugOn = isDebugOn;
        this.sessions = sessions;
        this.queueFactory = queueFactory;
    }
    
    private static StringDecoder STRINGDECODER = new StringDecoder();
    private static StringEncoder STRINGENCODER = new StringEncoder();
    private static HashedWheelTimer HASHEDWHEELTIMER = new HashedWheelTimer();
    private static LogHandler LOGHANDLER = new LogHandler();
    private static SimpleChannelUpstreamHandler NOOPHANDLER = new SimpleChannelUpstreamHandler();

    /*@Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipe = org.jboss.netty.channel.Channels.pipeline(
                new IdleStateHandler(new org.jboss.netty.util.HashedWheelTimer(), 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                isDebugOn ? LOGHANDLER : NOOPHANDLER,
                new FIXSessionProcessor(false,headerFields, trailerFields, logonManager, sessions,queueFactory),
                upstreamHandler
                );
        return pipe;
    }*/
    
    @Override
    public ChannelPipeline getPipeline() throws Exception {
    	final ChannelHandler[] handlers = {
    			new IdleStateHandler(HASHEDWHEELTIMER, 1, 1, 1),
                new FIXFrameDecoder(),
                STRINGDECODER,//Incoming
                STRINGENCODER,//Outgoing
                isDebugOn ? LOGHANDLER : NOOPHANDLER,
                new FIXSessionProcessor(true,headerFields, trailerFields,logonManager , sessions, queueFactory)};
        final ChannelHandler[] allHandlers = new ChannelHandler[handlers.length + upstreamHandlers.length];
        
        for(int i = 0; i < handlers.length; i++) allHandlers[i] = handlers[i];
        for(int i=0; i < upstreamHandlers.length; i++) allHandlers[handlers.length + i] = upstreamHandlers[i];
        
    	ChannelPipeline pipe = org.jboss.netty.channel.Channels.pipeline(allHandlers);
        return pipe;
    }
}
