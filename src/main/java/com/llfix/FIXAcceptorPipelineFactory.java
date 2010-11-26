package com.llfix;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.timeout.IdleStateHandler;

import com.llfix.handlers.FIXFrameDecoder;
import com.llfix.handlers.FIXMessageDecoder;
import com.llfix.handlers.FIXMessageEncoder;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.handlers.LogHandler;
import com.llfix.util.FieldAndRequirement;


public class FIXAcceptorPipelineFactory implements ChannelPipelineFactory{

    private final List<FieldAndRequirement> headerFields;
    private final List<FieldAndRequirement> trailerFields;
    private final ChannelUpstreamHandler upstreamHandler;
	private final ILogonManager logonManager;
	private final Set<String> sessions = new HashSet<String>();
	private final boolean isDebugOn;

    public FIXAcceptorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields){
        this(headerFields,trailerFields,true,new DefaultLogonManager(), new SimpleChannelUpstreamHandler());
    }
    
    public FIXAcceptorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields,
            final boolean isDebugOn,
            final ILogonManager logonManager,
            final ChannelUpstreamHandler upstreamHandler){
        this.headerFields = headerFields;
        this.trailerFields = trailerFields;
        this.logonManager = logonManager;
        this.upstreamHandler=upstreamHandler;
        this.isDebugOn = isDebugOn;
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
                new FIXMessageEncoder(headerFields, trailerFields),
                new FIXMessageDecoder(headerFields, trailerFields),
                new FIXSessionProcessor(false,headerFields, trailerFields, logonManager, sessions)
                );
        return pipe;
    }
}
