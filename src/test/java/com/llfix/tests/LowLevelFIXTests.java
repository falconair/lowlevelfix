package com.llfix.tests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.util.CharsetUtil;
import org.junit.Assert;
import org.junit.Test;

import com.llfix.handlers.FIXFrameDecoder;
import com.llfix.handlers.FIXSessionProcessor;
import com.llfix.util.DefaultLogonManager;
import com.llfix.util.FieldAndRequirement;
import com.llfix.util.SimpleQueueFactory;

public class LowLevelFIXTests {
	
	@Test
	public void testFIXSessionProcessor(){
		final DecoderEmbedder<Map<String,String>> h = new DecoderEmbedder<Map<String,String>>(
				new FIXSessionProcessor(
						true,
						new ArrayList<FieldAndRequirement>(),
						new ArrayList<FieldAndRequirement>(),
						new DefaultLogonManager(),
						new ConcurrentHashMap<String, Channel>(),
						new SimpleQueueFactory<String>()));
		
		final Map<String,String> fix = new HashMap<String, String>();
		fix.put("8", "FIX.4.2");

		h.offer(fix);
		
	}
	

	@Test
	public void testFIXFrameDecoder() {
		final DecoderEmbedder<String> h = new DecoderEmbedder<String>( 
				new FIXFrameDecoder(),
				new StringDecoder());

		final String fix1 = "8=FIX.4";
		final String fix2 = ".29=7135=A34=149=HTX_DC52=20080";
		final String fix3 = "912-13:23:19.55556=MPNLOMS4NJ98=0108=3010=208";

		h.offer(ChannelBuffers.copiedBuffer(fix1,CharsetUtil.US_ASCII));
		h.offer(ChannelBuffers.copiedBuffer(fix2,CharsetUtil.US_ASCII));
		h.offer(ChannelBuffers.copiedBuffer(fix3,CharsetUtil.US_ASCII));
		
		Assert.assertEquals(fix1+fix2+fix3,h.poll());
	}
}
