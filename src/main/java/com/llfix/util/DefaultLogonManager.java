package com.llfix.util;

import java.net.SocketAddress;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.llfix.ILogonManager;

public class DefaultLogonManager implements ILogonManager {
	
	final static Logger logger = LoggerFactory.getLogger(DefaultLogonManager.class);
	
	private final String senderCompID;

	public DefaultLogonManager(String senderCompID){
		this.senderCompID = senderCompID;
	}

	@Override
	public boolean allowLogon(SocketAddress remoteAddress, Map<String, String> logonMessage) {
		final String targetCompID = logonMessage.get("56");
		if(targetCompID.equals(senderCompID)) return true;

		logger.error("Expected target comp id "+senderCompID+" but received "+targetCompID);
		return false;
	}

}
