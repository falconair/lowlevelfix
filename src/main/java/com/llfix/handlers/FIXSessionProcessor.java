package com.llfix.handlers;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.timeout.IdleState;
import org.jboss.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.llfix.ILogonManager;
import com.llfix.util.FieldAndRequirement;

//TODO: Check all exceptions thrown by handlers, nothing should blow up the pipe!
//TODO: Log messages must all contains sender/target/ip/time expected/actual full message
//TODO: All map access, specifically fix.get access, may return null!
//TODO: seqnums should be long instead of int
//TODO: confirm that handlers can't be called by multiple threads
public class FIXSessionProcessor extends SimpleChannelHandler {

    final static Logger logger = LoggerFactory.getLogger(FIXSessionProcessor.class);
    final static char SOH_CHAR = '\001';

    private final List<FieldAndRequirement> headerFields;
    private final List<FieldAndRequirement> trailerFields;
	private final ILogonManager logonManager;
	private final boolean isInitiator;
	private final ConcurrentMap<String,Channel> sessions;

    private AtomicInteger outgoingSeqNum = new AtomicInteger(1);
    private AtomicInteger incomingSeqNum = new AtomicInteger(1);
    private boolean loggedIn = false;
    private boolean resendRequested = false;
    private AtomicBoolean isResending = new AtomicBoolean(false);
    
    private String fixVersion;
    private String senderCompID;
    private String targetCompID;
    private int heartbeatDuration;

    private Queue<Map<String,String>> outgoingMsgStore = new ConcurrentLinkedQueue<Map<String,String>>();
    //private Queue<String> incomingMsgStore = new ConcurrentLinkedQueue<String>();


    public FIXSessionProcessor(
    		final boolean isInitiator,
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields,
            final ILogonManager logonManager,
            final ConcurrentMap<String,Channel> sessions){

        this.headerFields = new ArrayList<FieldAndRequirement>(headerFields);//not a simple assignment because this list is mutated below
        this.trailerFields = trailerFields;
        this.logonManager = logonManager;
        this.isInitiator = isInitiator;
        this.sessions = sessions;
        
        //Tags 34,35 are required, even the client doesn't think they are
        this.headerFields.add(new FieldAndRequirement("34", true));
        this.headerFields.add(new FieldAndRequirement("35", true));
    }

    @SuppressWarnings("unchecked")
	@Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {
    	if(event instanceof MessageEvent){

    		final Map<String,String> fix = (Map<String,String>) ((MessageEvent)event).getMessage();
    		
    		if(loggedIn){
        		
        		outgoingMsgStore.add(fix);
        		
        		if(!isResending.get()){
            		fix.put("8", fixVersion);
            		fix.put("56", senderCompID);
            		fix.put("49", targetCompID);
            		fix.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));

            		Channels.write(ctx, Channels.future(ctx.getChannel()), fix);
        		}
    		}
    		else{
    			if(fix.get("35").equals("A")){
            		Channels.write(ctx, Channels.future(ctx.getChannel()), fix);    				
    			}
    			logger.error("Attempt to send a non-logon message, while not logged in: "+fix);
    			//TODO: send exception to sender
    		}
    	}
    	else{
            super.handleDownstream(ctx,event);
    	}

    }

    @SuppressWarnings("unchecked")
	@Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {


        if(event instanceof MessageEvent){
            Map<String,String> fix = (Map<String,String>) ((MessageEvent) event).getMessage();

            //====Step 4: Confirm all required fields are available====
            //Check required headers
            for (FieldAndRequirement far : headerFields) {
                if(!far.isRequired()) continue;
                String k = far.getTag();
                if (!fix.containsKey(k)) { //Does not contains a required field
                    logger.warn("Tag {} is required but missing in incoming message: {}", k, fix);
                    if (loggedIn) {
                    	final Map<String,String> rej = new HashMap<String, String>();
                		rej.put("8", fixVersion);
                		rej.put("56", senderCompID);
                		rej.put("49", targetCompID);
                    	rej.put("35", "3");
                    	rej.put("45", fix.get("34"));
                    	rej.put("373", "1");
                    	rej.put("58", String.format("Tag %s is required but missing", k));
                    	rej.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
                		Channels.write(ctx, Channels.future(ctx.getChannel()), rej);
                    } else {
                        ctx.getChannel().close();
                        return;
                    }
                }
            }

            //Check required trailers
            for (FieldAndRequirement far : trailerFields) {
                if(!far.isRequired()) continue;
                String k = far.getTag();
                if (!fix.containsKey(k)) { //Does not contains a required field
                    logger.warn("Tag {} is required but missing in incoming message: {}", k, fix);
                    if (loggedIn) {
                    	final Map<String,String> rej = new HashMap<String, String>();
                		rej.put("8", fixVersion);
                		rej.put("56", senderCompID);
                		rej.put("49", targetCompID);
                    	rej.put("35", "3");
                    	rej.put("45", fix.get("34"));
                    	rej.put("373", "1");
                    	rej.put("58", String.format("Tag %s is required but missing", k));
                    	rej.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
                		Channels.write(ctx, Channels.future(ctx.getChannel()), rej);
                    } else {
                        ctx.getChannel().close();
                        return;
                    }
                }
            }

            //====Step 5: Confirm first message is a logon message and it has a heartbeat

            final String msgType = fix.get("35");//MsgType

            if (!loggedIn && !msgType.equals("A")) {//Not logged in and received a non-login message
                logger.error("Expected logon message, but received: {}", fix);
                ctx.getChannel().close();
                return;
            }

            if (!loggedIn && msgType.equals("A")) {
                fixVersion = fix.get("8");
                senderCompID = fix.get("49");
                targetCompID = fix.get("56");
                heartbeatDuration = Integer.parseInt(fix.get("108"));
                
                if(!isInitiator && sessions.containsKey(senderCompID)){
                	logger.error("Multiple logons not allowed for sender comp ID {}: {}",senderCompID, fix);
                	ctx.getChannel().close();
                    return;
                }
                

                if(!logonManager.allowLogon(fix)){
                	logger.error("Logon not allowed: {}",fix);
                	ctx.getChannel().close();
                    return;
                }
                
                loggedIn = true;
                
                
                if(!isInitiator){
                	//logon ack
                	final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
            		outfixmap.put("8", fixVersion);
            		outfixmap.put("56", senderCompID);
            		outfixmap.put("49", targetCompID);

        			outfixmap.put("35", "A");
        			outfixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
        			outfixmap.put("98", "0"); //EncryptMethod=None
        			outfixmap.put("108", Integer.toString(heartbeatDuration));

                	sessions.put(senderCompID, ctx.getChannel());

                	Channels.write(ctx, Channels.future(ctx.getChannel()), outfixmap);
                }
                
                logger.info("{} logged on from {} with fix {}", new String[]{targetCompID, ctx.getChannel().getRemoteAddress().toString(),fixVersion});

            }


            //====Step 6: Confirm incoming sequence number====
            if (msgType.equals("4" /*sequence reset*/)
                    && (fix.get("123") == null || fix.get("123").equals("N"))) {//123=GapFillFlag

                logger.info("Sequence reset request received: {}", fix);
                final int resetSeqNo = Integer.parseInt(fix.get("36"));

                if (resetSeqNo <= incomingSeqNum.get()) {
                	final String error = String.format("Sequence reset request may only increment sequence number current seqno=%s, reset req=%s",incomingSeqNum.get(),resetSeqNo);
                	logger.error(error);
                    
                	final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
            		outfixmap.put("8", fixVersion);
            		outfixmap.put("56", senderCompID);
            		outfixmap.put("49", targetCompID);

        			outfixmap.put("35", "3"); //Session Reject
        			outfixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
        			outfixmap.put("45", fix.get("34")); //RefSeqNum
        			outfixmap.put("58", error);
        			
                	Channels.write(ctx, Channels.future(ctx.getChannel()), outfixmap);
                } else {
                    incomingSeqNum.set(resetSeqNo);
                }
            }


            final int msgSeqNum = Integer.parseInt(fix.get("34"));

            if (msgSeqNum == incomingSeqNum.get()) {
                incomingSeqNum.getAndIncrement();
                resendRequested = false;
            } else if (msgSeqNum < incomingSeqNum.get()) {
                final String posDupStr = fix.get("43");
                final boolean isPosDup = posDupStr==null? false : posDupStr.equals("Y") ? true : false;

                if (isPosDup) {
                    logger.info("This posdup message's seqno has already been processed.  Application must handle: {}", fix);
                    //return;
                } else {
                    logger.warn("Incoming sequence number lower than expected. No way to recover message: {}", fix);
                    ctx.getChannel().close();
                    return;
                }
            } else if (msgSeqNum > incomingSeqNum.get()) {
                //Missing messages, write resend request and don't process any more messages
                //until the resend request is processed
                //set flag signifying "waiting for resend"
                if (resendRequested != true) {
                	final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
            		outfixmap.put("8", fixVersion);
            		outfixmap.put("56", senderCompID);
            		outfixmap.put("49", targetCompID);

        			outfixmap.put("35", "2"); //Session Reject
        			outfixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
        			outfixmap.put("7", Integer.toString(incomingSeqNum.get())); //BeginSeqNo
        			outfixmap.put("16", "0"); //EndSeqno
        			
                	Channels.write(ctx, Channels.future(ctx.getChannel()), outfixmap);
                    resendRequested = true;
                }
            }

            //====Step 7: Confirm compids and fix version match what was in the logon msg
            final String infixVersion = fix.get("8");
            final String insenderCompID = fix.get("49");
            final String intargetCompID = fix.get("56");
            
            if(!fixVersion.equals(infixVersion) || !senderCompID.equals(insenderCompID) || !targetCompID.equals(intargetCompID)){
            	final String error = String.format("FIX Version, Sender and Target CompIDs do not match expected values: Version=%s, SenderCompID=%s, TargetCompID=%s in msg=%s",fixVersion,senderCompID,targetCompID,fix);
            	logger.error(error);
                
            	final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
        		outfixmap.put("8", fixVersion);
        		outfixmap.put("56", senderCompID);
        		outfixmap.put("49", targetCompID);

    			outfixmap.put("35", "3"); //Session Reject
    			outfixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
    			outfixmap.put("45", fix.get("34")); //RefSeqNum
    			outfixmap.put("58", error);
    			
            	Channels.write(ctx, Channels.future(ctx.getChannel()), outfixmap);
            }
            

            //===Step 8: Record incoming message -- might be needed during resync
            //incomingMsgStore.add(fix);
            //TODO Writing messages to disk should be done outside this module
            //When messages are read in from disk during recovery, messages must be annotated as such
            //(perhaps by setting posdup to true) to avoid having the engine take action on possibly expired messages

            //====Step 9: Handle messages
            if (msgType.equals("0")) {//Heartbeat
                //Nothing to do, IdleStateEvent takes care of this
            } else if(msgType.equals("A")){//Logon
            	//Nothing to do, handled in step 5
            } else if (msgType.equals("1")) {//TestRequest
            	final String TestReqID = fix.get("112");
            	
            	final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
        		outfixmap.put("8", fixVersion);
        		outfixmap.put("56", senderCompID);
        		outfixmap.put("49", targetCompID);

    			outfixmap.put("35", "0");
    			outfixmap.put("112", TestReqID);
    			outfixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
    			
    			Channels.write(ctx, Channels.future(ctx.getChannel()), fix);
            } else if (msgType.equals("2")) {//ResendRequest
                isResending.set(true);
                final String startSeqStr = fix.get("7");
                final String endSeqStr = fix.get("16");
                
                final int startSeq = Integer.parseInt(startSeqStr);
                final int endSeq = endSeqStr.equals("0")? Integer.MAX_VALUE : Integer.parseInt(endSeqStr);
                
                for(Map<String,String> oldfix : outgoingMsgStore){
                	final String seqNumStr = oldfix.get("34");
                	final int seqNum = Integer.parseInt(seqNumStr);
                	
                	if(seqNum >= startSeq && seqNum <= endSeq){
                		final Map<String,String> newfix = new LinkedHashMap<String, String>(oldfix);
                		newfix.put("97", "Y");//PosResend
                		newfix.put("43", "Y");
                		newfix.put("122", oldfix.get("52"));
                		Channels.write(ctx, Channels.future(ctx.getChannel()), newfix);
                	}
                	isResending.set(false);
                }
                
            } else if (msgType.equals("3")) {//SessionReject
                logger.error("Session reject! message="+fix);
            } else if (msgType.equals("4")) {//SequenceReset
                //Taken care of in step 6
            } else if (msgType.equals("5")) {//LogOut
            	final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
        		outfixmap.put("8", fixVersion);
        		outfixmap.put("56", senderCompID);
        		outfixmap.put("49", targetCompID);

    			outfixmap.put("35", "5");
    			outfixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
    			
    			Channels.write(ctx, Channels.future(ctx.getChannel()), outfixmap);
    			
    			loggedIn = false;
    			if(!isInitiator){
    				sessions.remove(senderCompID);
    			}
            }
            //else{//commented out because just send ALL events on, no need to stop here?
            	//Not needed by the session logic, send it on
            	ctx.sendUpstream(event);
            //}
        	
        }
        else if(event instanceof IdleStateEvent && ((IdleStateEvent)event).getState() == IdleState.WRITER_IDLE){
			long currentTime = System.currentTimeMillis();
			long lastActivity = ((IdleStateEvent)event).getLastActivityTimeMillis();
			
			/*logger.debug("Time passed={}, heartbeat duration={}, loggedin={}",
					new String[]{
						Long.toString(currentTime-lastActivity),
						Integer.toString(heartbeatDuration*1000),
						Boolean.toString(loggedIn)});*/
				
			if(loggedIn && (currentTime-lastActivity > heartbeatDuration * 1000)){
				Map<String,String> fixmap = new LinkedHashMap<String, String>();
	    		fixmap.put("8", fixVersion);
	    		fixmap.put("56", senderCompID);
	    		fixmap.put("49", targetCompID);

				fixmap.put("35", "0");
				fixmap.put("34", Integer.toString(outgoingSeqNum.getAndIncrement()));
				
				Channels.write(ctx, Channels.future(ctx.getChannel()), fixmap);
			}
		}
        else{
        	super.handleUpstream(ctx, event);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.warn("FIXSessionProcessor",e.getCause());
        super.exceptionCaught(ctx, e);
    }

    public static final String checksum(final CharSequence str) {
        int val = 0;
        for (int i = 0; i < str.length(); i++) {
            val += str.charAt(i);
        }
        final int checksum = val % 256;

        if (checksum >= 0 && checksum < 10) {
            return "00" + checksum;
        } else if (checksum >= 10 && checksum < 100) {
            return "0" + checksum;
        } else {
            return Integer.toString(checksum);
        }
    }


    public static Map<String, String> decode(final String fix) throws ParseException {
        final Map<String, String> map = new LinkedHashMap<String, String>();
        final List<String> attributes = fastSplitAll(fix, SOH_CHAR);
        int count = 0;
        for (final String attr : attributes) {
            count++;
            final String[] keyVal = fastSplit(attr, '=');

            final String tag = keyVal[0];
            final String value = keyVal[1];
            if (tag == null || tag.equals("")) {
                throw new ParseException(String.format("Tag at position [%d] is empty: [%s]", count, attr), count);
            }
            if (value == null || value.equals("")) {
                throw new ParseException(String.format("Tag [%s] at position [%d] has no value: [%s]", tag, count, attr), count);
            }
            map.put(tag, value);
        }
        return map;
    }

    public static final String[] fastSplit(final String s, final char delim) {
        final int index = s.indexOf(delim, 0);
        if (index < 0) {
            return new String[]{s, ""};
        }
        if (index > s.length()) {
            return new String[]{s, ""};
        }
        final String left = s.substring(0, index);
        final String right = s.substring(index + 1);

        return new String[]{left, right};
    }

    public static final List<String> fastSplitAll(final String s, final char delim) {
        final List<String> l = new ArrayList<String>();
        int index = -1;
        int oldindex = 0;
        while ((index = s.indexOf(delim, oldindex)) != -1) {
            String temp = s.substring(oldindex, index);
            l.add(temp);
            oldindex = index + 1;
        }
        l.add(s.substring(oldindex, s.length()));
        return l;
    }
}
