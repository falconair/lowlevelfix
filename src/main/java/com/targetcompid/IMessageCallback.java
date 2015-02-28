package com.targetcompid;


public interface IMessageCallback<E> {

	public void onMsg(E msg);
	public void onException(Throwable t);
}
