package com.llfix;

import java.util.Map;

public interface IMessageCallback {

	public void onMsg(Map<String,String> msg);
	public void onException(Throwable t);
}
