package com.llfix.handlers;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamMessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FIXMessageDecoder extends SimpleChannelHandler {

    final static Logger logger = LoggerFactory.getLogger(FIXMessageDecoder.class);
    final static char SOH_CHAR = '\001';

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent event) throws Exception {
        if (!(event instanceof MessageEvent)) {
            super.handleUpstream(ctx, event);
            return;
        }

        String msg = (String) ((MessageEvent) event).getMessage();

        //====Step 2: Validate message====
        final int _length = msg.length();
        final String calculatedChecksum = checksum(msg.substring(0, _length - 7));
        final String extractedChecksum = msg.substring(_length - 4, _length - 1);

        if (!calculatedChecksum.equals(extractedChecksum)) {
            logger.warn(String.format("Extracted checksum (%s) does not match calculated checksum (%s). Dropping malformed message: %s", extractedChecksum, calculatedChecksum, msg));
        }

        //====Step 3: Convert to map====
        final Map<String, String> fix = decode(msg);

        final UpstreamMessageEvent nextEvent = new UpstreamMessageEvent(ctx.getChannel(), fix, null);
        ctx.sendUpstream(nextEvent);

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
}
