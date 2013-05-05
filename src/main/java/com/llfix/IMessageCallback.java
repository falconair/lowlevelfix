package com.llfix;


public interface IMessageCallback<E> {

	public void onMsg(E msg);
	public void onException(Throwable t);
}
