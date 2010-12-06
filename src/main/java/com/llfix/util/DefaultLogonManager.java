package com.llfix.util;

import java.net.SocketAddress;
import java.util.Map;

import com.llfix.ILogonManager;

public class DefaultLogonManager implements ILogonManager {

	@Override
	public boolean allowLogon(SocketAddress remoteAddress, Map<String, String> logonMessage) {
		return true;
	}

}
