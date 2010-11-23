package com.llfix.handlers;

import com.llfix.util.FieldAndRequirement;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FIXMessageEncoder extends SimpleChannelHandler {

    final static Logger logger = LoggerFactory.getLogger(FIXSessionProcessor.class);

    final static DateTimeFormatter UTCTimeStampFormat = DateTimeFormat.forPattern("yyyyMMdd-HH:mm:ss.SSS");
    final static DateTimeZone UTCTimeZone = DateTimeZone.forOffsetHours(0);
    final static char SOH_CHAR = '\001';

    private final List<FieldAndRequirement> headerFields;
    private final List<FieldAndRequirement> trailerFields;

    public FIXMessageEncoder(
            final List<FieldAndRequirement> headerFields,
            final List<FieldAndRequirement> trailerFields){

        this.headerFields = headerFields;
        this.trailerFields = trailerFields;
    }

    @SuppressWarnings("unchecked")
	@Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {
        if (!(event instanceof MessageEvent)) {
            super.handleDownstream(ctx, event);
            return;
        }

        final MessageEvent msgEvent = (MessageEvent) event;
        final Map<String,String> fixMap = (Map<String, String>) msgEvent.getMessage();
        
        final String fix = encodeAndCalcChksmCalcBodyLen(fixMap, headerFields, trailerFields);

        Channels.write(ctx, Channels.future(ctx.getChannel()), fix);

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.warn("FIXMessageEncoder",e.getCause());
        super.exceptionCaught(ctx, e);
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
}
