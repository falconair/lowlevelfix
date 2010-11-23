package com.llfix;

import java.util.Map;

public class DefaultLogonManager implements ILogonManager {

	@Override
	public boolean allowLogon(Map<String, String> logonMessage) {
		return true;
	}

}
