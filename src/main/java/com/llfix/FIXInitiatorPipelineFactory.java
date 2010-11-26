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


public class FIXInitiatorPipelineFactory implements ChannelPipelineFactory{

    private final List<FieldAndRequirement> headerFields;
    private final List<FieldAndRequirement> trailerFields;
	private final Set<String> sessions = new HashSet<String>();
	private final ChannelUpstreamHandler upstreamHandler;
	private final boolean isDebugOn;
	
	private final static ILogonManager logOnManager = new DefaultLogonManager();


    public FIXInitiatorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields){
    	this(headerFields,trailerFields, new SimpleChannelUpstreamHandler(), false);
    }

    public FIXInitiatorPipelineFactory(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields,
            final ChannelUpstreamHandler upstreamHandler,
            final boolean isDebugOn){
        this.headerFields = headerFields;
        this.trailerFields = trailerFields;
        this.upstreamHandler = upstreamHandler;
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
                new FIXMessageDecoder(),
                new FIXSessionProcessor(true,headerFields, trailerFields,logOnManager , sessions),
                upstreamHandler);
        return pipe;
    }
}
