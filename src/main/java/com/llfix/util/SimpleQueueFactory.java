package com.llfix.util;


public class SimpleQueueFactory<T> implements IQueueFactory<T> {

	@Override
	public ISimpleQueue<T> getQueue(String name) {
		return new SimpleQueue<T>();
	}

	@Override
	public void returnQueue(String name) {
		
	}

}
