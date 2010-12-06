package com.llfix;

import java.net.SocketAddress;
import java.util.Map;

public interface ILogonManager {

	public boolean allowLogon(SocketAddress remoteAddress, Map<String,String> logonMessage);
}
