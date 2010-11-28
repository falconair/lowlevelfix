package com.llfix.handlers;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO Instead of decoding byte frame, is it better to convert it to string first, then operate on strings?
public final class FIXFrameDecoder extends FrameDecoder{
    
    static final Logger logger = LoggerFactory.getLogger(FIXFrameDecoder.class);

    private final static int maxlength = 5;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Make sure if the length field was received.
        if (buf.readableBytes() <= 13) {
            // The length field was not received yet - return null.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.
            return null;
        }
        
        final int endOfLength = buf.indexOf(12, buf.readableBytes(), (byte) '\01');
        if(endOfLength==-1){
            if(buf.readableBytes()>maxlength){
                //too many characters read, but no length field found
                throw new TooLongFrameException("End of length field not found within "+(maxlength+12)+" bytes");
            }
            //end of length field not found
            return null;
        }
        
        final String lengthStr = buf.slice(12, endOfLength-12).toString(Charset.forName("US-ASCII"));
        
        final int length = Integer.parseInt(lengthStr);
        
        final int totalLength = length+endOfLength+8;
        
        if(totalLength>9999){
            throw new TooLongFrameException("Frame length may not be greater than 9999 bytes");
        }

        if (buf.readableBytes() < totalLength) {
            return null;
        }

        // There are enough bytes in the buffer. Read it.
        final ChannelBuffer frame = buf.readBytes(totalLength);

        // Successfully decoded a frame.  Return the decoded frame.
        return frame;
    }

}
