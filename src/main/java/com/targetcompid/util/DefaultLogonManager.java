package com.targetcompid.util;

import java.net.InetAddress;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.targetcompid.ILogonManager;

public class DefaultLogonManager implements ILogonManager {
	
	final static Logger logger = LoggerFactory.getLogger(DefaultLogonManager.class);
	
	private final String senderCompID;

	public DefaultLogonManager(String senderCompID){
		this.senderCompID = senderCompID;
	}

	@Override
	public boolean allowLogon(InetAddress remoteAddress, Map<String, String> logonMessage) {
		final String targetCompID = logonMessage.get("56");
		if(targetCompID.equals(senderCompID)) return true;

		logger.error("Expected target comp id "+senderCompID+" but received "+targetCompID);
		return false;
	}

}
