package com.targetcompid;

import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.Map;

public interface ILogonManager {

	public boolean allowLogon(InetAddress remoteAddress, Map<String,String> logonMessage);
}
