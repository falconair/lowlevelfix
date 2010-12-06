package com.llfix.util;

import java.util.Map;

import com.llfix.ILogonManager;

public class DefaultLogonManager implements ILogonManager {

	@Override
	public boolean allowLogon(Map<String, String> logonMessage) {
		return true;
	}

}
