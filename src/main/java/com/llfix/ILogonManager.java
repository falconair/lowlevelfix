package com.llfix;

import java.util.Map;

public interface ILogonManager {

	public boolean allowLogon(Map<String,String> logonMessage);
}
