package com.llfix.util;


public interface IQueueFactory<T> {

	public ISimpleQueue<T> getQueue(String name) throws Exception;
	public void returnQueue(String name);
}
