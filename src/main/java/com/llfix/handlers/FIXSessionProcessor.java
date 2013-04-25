package com.llfix.handlers;

import java.net.InetAddress;
import java.net.SocketAddress;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.llfix.*;
import com.llfix.util.FieldAndRequirement;

public class FIXSessionProcessor {

	final static Logger logger = LoggerFactory.getLogger(FIXSessionProcessor.class);
	final static char SOH_CHAR = '\001';
	final static DateTimeFormatter UTCTimeStampFormat = DateTimeFormat.forPattern("yyyyMMdd-HH:mm:ss.SSS");
	final static DateTimeZone UTCTimeZone = DateTimeZone.forOffsetHours(0);

	private final ILogonManager logonManager;
	private final boolean isInitiator;

	private long outgoingSeqNum = 1L;
	private long incomingSeqNum = 1L;
	private boolean loggedIn = false;
	private boolean resendRequested = false;
	private AtomicBoolean isResending = new AtomicBoolean(false);

	private String fixVersion;
	private String senderCompID;
	private String targetCompID;
	private int heartbeatDuration;

	private final IQueueFactory<String> qFactory;
	private final IMessageCallback outgoingCallback;
	private final IMessageCallback incomingCallback;
	private final SocketAddress remoteAddress;
	private Set<String> sessions;
	private ISimpleQueue<String> msgStore;




	public FIXSessionProcessor(
			final boolean isInitiator,
			final ILogonManager logonManager,
			final Set<String> sessions,
			final IQueueFactory<String> qFactory,
			final IMessageCallback outgoingCallback,
			final IMessageCallback incomingCallback,
			final SocketAddress remoteAddress){

		this.logonManager = logonManager;
		this.isInitiator = isInitiator;
		this.sessions= sessions;
		this.qFactory = qFactory;
		this.outgoingCallback = outgoingCallback;
		this.incomingCallback = incomingCallback;
		this.remoteAddress = remoteAddress;

	}
	
	public void processOutgoing(Map<String,String> fix) throws Exception {


			if(loggedIn){

				if(!isResending.get()){
					fix.put("8", fixVersion);
					fix.put("56", senderCompID);
					fix.put("49", targetCompID);
					fix.put("34", Long.toString(outgoingSeqNum));
					outgoingSeqNum++;

					write(fix);
				}
			}
			else{
				if(fix.get("35").equals("A")){

					final String senderCompID = fix.get("56");
					final String targetCompID = fix.get("49");

					msgStore = qFactory.getQueue(senderCompID+"-"+targetCompID);
					for(String oldMsgStr : msgStore){
						final Map<String,String> oldMsg = decode(oldMsgStr);
						if(oldMsg.get("49").equals(targetCompID)){
							//IF this was an outgoing message
							outgoingSeqNum = Long.parseLong(oldMsg.get("34"))+1;
						}
						else if(oldMsg.get("49").equals(senderCompID)){
							//IF this was an incoming message
							incomingSeqNum = Long.parseLong(oldMsg.get("34"))+1;
						}
					}

					fix.put("34", Long.toString(outgoingSeqNum));
					outgoingSeqNum++;

					write(fix);				
				}
				else{
					logger.error(senderCompID+"->"+targetCompID+":Attempt to send a non-logon message, while not logged in: "+fix);
				}
				//TODO: send exception to sender
			}

	}

	public void processIncoming(String msg) throws Exception {


			//====Step 2: Validate message====
			final int _length = msg.length();
			final String calculatedChecksum = checksum(msg.substring(0, _length - 7));
			final String extractedChecksum = msg.substring(_length - 4, _length - 1);

			if (!calculatedChecksum.equals(extractedChecksum)) {
				logger.warn(String.format("Extracted checksum (%s) does not match calculated checksum (%s). Dropping malformed message: %s", extractedChecksum, calculatedChecksum, msg));
				return;
			}

			//====Step 3: Convert to map====
			final Map<String, String> fix = decode(msg);

			//====Step 4: Confirm all required fields are available====
			//Check required headers
			for (String k : new String[]{"34","35"}) {
				if (!fix.containsKey(k)) { //Does not contain a required field
					logger.warn(String.format("%s->%s: Tag %s is required but missing in incoming message: %s",senderCompID,targetCompID, k, fix));
					if (loggedIn) {
						final Map<String,String> rej = new HashMap<String, String>();
						rej.put("8", fixVersion);
						rej.put("56", senderCompID);
						rej.put("49", targetCompID);
						rej.put("35", "3");
						rej.put("45", fix.get("34"));
						rej.put("373", "1");
						rej.put("58", String.format("Tag %s is required but missing", k));
						rej.put("34", Long.toString(outgoingSeqNum));
						write(rej);
						outgoingSeqNum++;

					} else {
						closeSession();
						return;
					}
				}
			}

			//Check required trailers
			for (String k : new String[]{"10"}) {
				if (!fix.containsKey(k)) { //Does not contain a required field
					logger.warn(String.format("%s->%s: Tag %s is required but missing in incoming message: %s",senderCompID,targetCompID, k, fix));
					if (loggedIn) {
						final Map<String,String> rej = new HashMap<String, String>();
						rej.put("8", fixVersion);
						rej.put("56", senderCompID);
						rej.put("49", targetCompID);
						rej.put("35", "3");
						rej.put("45", fix.get("34"));
						rej.put("373", "1");
						rej.put("58", String.format("Tag %s is required but missing", k));
						rej.put("34", Long.toString(outgoingSeqNum));
						outgoingSeqNum++;
						write(rej);
					} else {
						closeSession();
						return;
					}
				}
			}

			//====Step 5: Confirm first message is a logon message and it has a heartbeat

			final String msgType = fix.get("35");//MsgType

			if (!loggedIn && !msgType.equals("A")) {//Not logged in and received a non-login message
				logger.error("Expected logon message, but received: {}", fix);
				closeSession();
				return;
			}

			if (!loggedIn && msgType.equals("A")) {
				fixVersion = fix.get("8");
				senderCompID = fix.get("49");
				targetCompID = fix.get("56");
				heartbeatDuration = Integer.parseInt(fix.get("108"));

				if(!isInitiator && sessions.contains(senderCompID)){
					logger.error("Multiple logons not allowed for sender comp ID {}: {}",senderCompID, fix);
					closeSession();
					return;
				}


				if(!logonManager.allowLogon(remoteAddress,fix)){
					logger.error(String.format("%s->%s: Logon not allowed: %s",senderCompID, targetCompID, fix));
					closeSession();
					return;
				}

				loggedIn = true;

				if(!isInitiator) msgStore = qFactory.getQueue(senderCompID+"-"+targetCompID);

				for(String oldMsgStr : msgStore){
					final Map<String,String> oldMsg = decode(oldMsgStr);
					if(oldMsg.get("49").equals(targetCompID)){
						//IF this was an outgoing message
						outgoingSeqNum = Long.parseLong(oldMsg.get("34"))+1;
					}
					else if(oldMsg.get("49").equals(senderCompID)){
						//IF this was an incoming message
						incomingSeqNum = Long.parseLong(oldMsg.get("34"))+1;
					}
				}


				if(!isInitiator){
					//logon ack
					final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
					outfixmap.put("8", fixVersion);
					outfixmap.put("56", senderCompID);
					outfixmap.put("49", targetCompID);

					outfixmap.put("35", "A");
					outfixmap.put("34", Long.toString(outgoingSeqNum));
					outgoingSeqNum++;
					outfixmap.put("98", "0"); //EncryptMethod=None
					outfixmap.put("108", Integer.toString(heartbeatDuration));

					sessions.add(senderCompID);

					write(outfixmap);
				}

				logger.info("{} logged on from {} with fix {}", new String[]{targetCompID, remoteAddress.toString(),fixVersion});

			}


			//====Step 6: Confirm incoming sequence number====
			if (msgType.equals("4" /*sequence reset*/)
					&& (fix.get("123") == null || fix.get("123").equals("N"))) {//123=GapFillFlag

				//logger.info("Sequence reset request received: {}", fix);
				final long resetSeqNo = Long.parseLong(fix.get("36"));

				if (resetSeqNo <= incomingSeqNum) {
					final String error = String.format("%s->%s: Sequence reset request may only increment sequence number current seqno=%s, reset req=%s",senderCompID,targetCompID,incomingSeqNum,resetSeqNo);
					logger.error(error);

					final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
					outfixmap.put("8", fixVersion);
					outfixmap.put("56", senderCompID);
					outfixmap.put("49", targetCompID);

					outfixmap.put("35", "3"); //Session Reject
					outfixmap.put("34", Long.toString(outgoingSeqNum));
					outgoingSeqNum++;
					outfixmap.put("45", fix.get("34")); //RefSeqNum
					outfixmap.put("58", error);

					write(outfixmap);
				} else {
					incomingSeqNum = resetSeqNo;
				}
			}


			final int msgSeqNum = Integer.parseInt(fix.get("34"));

			if (msgSeqNum == incomingSeqNum) {
				incomingSeqNum++;
				resendRequested = false;
			} else if (msgSeqNum < incomingSeqNum) {
				final String posDupStr = fix.get("43");
				final boolean isPosDup = posDupStr==null? false : posDupStr.equals("Y") ? true : false;

				if (isPosDup) {
					logger.info(String.format("%s->%s: This posdup message's seqno has already been processed.  Application must handle: %s",senderCompID,targetCompID, fix));
					return; //TODO: how should posdups be handled?
				} else {
					logger.warn(String.format("%s->%s: Incoming sequence number lower than expected. No way to recover message: %s",senderCompID,targetCompID, fix));
					closeSession();
					return;
				}
			} else if (msgSeqNum > incomingSeqNum) {
				//Missing messages, write resend request and don't process any more messages
				//until the resend request is processed
				//set flag signifying "waiting for resend"
				if (!resendRequested) {
					final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
					outfixmap.put("8", fixVersion);
					outfixmap.put("56", senderCompID);
					outfixmap.put("49", targetCompID);

					outfixmap.put("35", "2"); //Session Reject
					outfixmap.put("34", Long.toString(outgoingSeqNum));
					outgoingSeqNum++;
					outfixmap.put("7", Long.toString(incomingSeqNum)); //BeginSeqNo
					outfixmap.put("16", "0"); //EndSeqno

					write(outfixmap);
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
				outfixmap.put("34", Long.toString(outgoingSeqNum));
				outgoingSeqNum++;
				outfixmap.put("45", fix.get("34")); //RefSeqNum
				outfixmap.put("58", error);

				write(outfixmap);
			}


			//===Step 8: Record incoming message -- might be needed during resync
			msgStore.offer(msg);
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
				outfixmap.put("34", Long.toString(outgoingSeqNum));
				outgoingSeqNum++;

				write(outfixmap);
			} else if (msgType.equals("2")) {//ResendRequest
				isResending.set(true);
				final String startSeqStr = fix.get("7");
				final String endSeqStr = fix.get("16");

				final int startSeq = Integer.parseInt(startSeqStr);
				final int endSeq = endSeqStr.equals("0")? Integer.MAX_VALUE : Integer.parseInt(endSeqStr);

				for(String oldFixStr : msgStore){
					final Map<String,String> oldfix = decode(oldFixStr);
					//confirm target compid to ignore incoming messages in the queue
					if(!oldfix.get("49").equals(targetCompID)) continue;
					final String seqNumStr = oldfix.get("34");
					final int seqNum = Integer.parseInt(seqNumStr);

					if(seqNum >= startSeq && seqNum <= endSeq){
						final Map<String,String> newfix = new LinkedHashMap<String, String>(oldfix);
						newfix.put("97", "Y");//PosResend
						newfix.put("43", "Y");
						newfix.put("122", oldfix.get("52"));

						write(newfix);
					}
					isResending.set(false);
				}

			} else if (msgType.equals("3")) {//SessionReject
				logger.error(String.format("%s->%s: Session reject message: %s",senderCompID,targetCompID,fix));
			} else if (msgType.equals("4")) {//SequenceReset
				//Taken care of in step 6
			} else if (msgType.equals("5")) {//LogOut
				final Map<String,String> outfixmap = new LinkedHashMap<String, String>();
				outfixmap.put("8", fixVersion);
				outfixmap.put("56", senderCompID);
				outfixmap.put("49", targetCompID);

				outfixmap.put("35", "5");
				outfixmap.put("34", Long.toString(outgoingSeqNum));
				outgoingSeqNum++;

				write(outfixmap);

				loggedIn = false;
				if(!isInitiator){
					sessions.remove(senderCompID);
				}
				else{
					//If initiator, then after receiving logoff confirm, disconnect
					forceDisconnect();
				}
			}

			forwardToApp(fix);


		/*if(event instanceof IdleStateEvent && ((IdleStateEvent)event).getState() == IdleState.WRITER_IDLE){
			long currentTime = System.currentTimeMillis();
			long lastActivity = ((IdleStateEvent)event).getLastActivityTimeMillis();

			if(loggedIn && (currentTime-lastActivity > heartbeatDuration * 1000)){
				Map<String,String> fixmap = new LinkedHashMap<String, String>();
				fixmap.put("8", fixVersion);
				fixmap.put("56", senderCompID);
				fixmap.put("49", targetCompID);

				fixmap.put("35", "0");
				fixmap.put("34", Long.toString(outgoingSeqNum));
				outgoingSeqNum++;

				write(fixmap);
			}
		}*/
	}
	
	private void forceDisconnect() {
		
	}

	private void closeSession(){
		
	}
	
	private void forwardToApp(Map<String,String> msg){
		incomingCallback.onMsg(msg);
	}

	private void write(final Map<String, String> msg)
			throws Exception {
		final String fixstr = encodeAndCalcChksmCalcBodyLen(msg, headerFields, trailerFields);
		msgStore.offer(fixstr);
		outgoingCallback.onMsg(msg);
		//Channels.write(Channels.future(ctx.getChannel()), fixstr);
	}

	@Override
	protected void finalize() throws Throwable {
		sessions.remove(senderCompID);
		qFactory.returnQueue(senderCompID+"-"+targetCompID);
		super.finalize();
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
				throw new ParseException(String.format("Tag at position [%d] is empty: [%s]: %s", count, attr,fix), count);
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
		//l.add(s.substring(oldindex, s.length()));
		return l;
	}

	public static String encodeAndCalcChksmCalcBodyLen(final Map<String, String> map, final List<FieldAndRequirement> headerFields, final List<FieldAndRequirement> trailerFields) {
		final Map<String, String> headerMap = new LinkedHashMap<String, String>();
		final Map<String, String> trailerMap = new LinkedHashMap<String, String>();

		map.remove("9");//Remove body length tag
		map.remove("10");//Remove checksum tag
		map.remove("52");//Remove time stamp tag

		final String beginString = map.remove("8");
		if (beginString == null) {
			//TODO: Missing required session tag exception
			throw new RuntimeException("FIX version (tag 8) not found for message: " + map);
		}

		final String msgType = map.remove("35");
		if (msgType == null) {
			//TODO: Missing required session tag exception
			throw new RuntimeException("FIX MsgType (tag 35) not found for message: " + map);
		}


		StringBuilder header = new StringBuilder();
		header.append("35=").append(msgType).append(SOH_CHAR);//After, tags 8 and 9, tag 35 must be the first header tag
		for (FieldAndRequirement fields : headerFields) {
			final String tag = fields.getTag();

			final String val = map.remove(tag);
			if (val == null && fields.isRequired() && (!tag.equals("8")) && (!tag.equals("9")) && (!tag.equals("35")) && (!tag.equals("10")) && (!tag.equals("52"))) {
				throw new RuntimeException("Tag [" + tag + "] missing in message " + map);
			}
			if (val == null) {
				continue;
			}

			headerMap.put(tag, val);

			header.append(tag).append('=').append(val).append(SOH_CHAR);
		}

		header.append("52=").append(new DateTime().withZone(UTCTimeZone).toString(UTCTimeStampFormat)).append(SOH_CHAR);


		StringBuilder trailer = new StringBuilder();
		for (FieldAndRequirement fields : trailerFields) {
			final String tag = fields.getTag();

			final String val = map.remove(tag);
			if (val == null && fields.isRequired() && (!tag.equals("8")) && (!tag.equals("9")) && (!tag.equals("10")) && (!tag.equals("52"))) {
				throw new RuntimeException("Tag [" + tag + "] missing in message " + map);
			}
			if (val == null) {
				continue;
			}

			trailerMap.put(tag, val);

			trailer.append(tag).append('=').append(val).append(SOH_CHAR);
		}

		StringBuilder body = new StringBuilder();
		for (Entry<String, String> entry : map.entrySet()) {
			final String tag = entry.getKey();
			final String val = entry.getValue();

			body.append(tag).append('=').append(val).append(SOH_CHAR);
		}

		StringBuilder fix = new StringBuilder();
		fix.append("8=").append(beginString).append(SOH_CHAR);
		fix.append("9=").append(header.length() + body.length() + trailer.length()).append(SOH_CHAR);
		fix.append(header);
		fix.append(body);
		fix.append(trailer);
		int checksum = 0;
		for (int i = 0; i < fix.length(); i++) {
			checksum += fix.charAt(i);
		}
		fix.append("10=").append(checksumToString(checksum % 256)).append(SOH_CHAR);

		return fix.toString();
	}

	public static final String checksumToString(int checksum) {
		if (checksum > 0 && checksum < 10) {
			return "00" + checksum;
		} else if (checksum >= 10 && checksum < 100) {
			return "0" + checksum;
		} else {
			return Integer.toString(checksum);
		}
	}
}
