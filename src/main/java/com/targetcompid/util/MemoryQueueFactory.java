package com.targetcompid.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.targetcompid.IQueueFactory;
import com.targetcompid.ISimpleQueue;


public class MemoryQueueFactory<T> implements IQueueFactory<T> {
	
	private final ConcurrentMap<String,ISimpleQueue<T>> lookup = new ConcurrentHashMap<String, ISimpleQueue<T>>();

	@Override
	public ISimpleQueue<T> getQueue(String name) {
		
		lookup.putIfAbsent(name, new MemoryQueue<T>());
		return lookup.get(name);
	}

	@Override
	public void returnQueue(String name) {
		
	}

}
