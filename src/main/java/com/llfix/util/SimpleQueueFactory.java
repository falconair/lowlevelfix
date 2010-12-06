package com.llfix.util;

import com.llfix.IQueueFactory;
import com.llfix.ISimpleQueue;


public class SimpleQueueFactory<T> implements IQueueFactory<T> {

	@Override
	public ISimpleQueue<T> getQueue(String name) {
		return new SimpleQueue<T>();
	}

	@Override
	public void returnQueue(String name) {
		
	}

}
