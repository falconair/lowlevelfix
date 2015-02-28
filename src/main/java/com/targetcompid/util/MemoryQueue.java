package com.targetcompid.util;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.targetcompid.ISimpleQueue;

public final class MemoryQueue<E> implements ISimpleQueue<E> {

	private final Queue<E> q = new ConcurrentLinkedQueue<E>();
	
	@Override
	public void offer(E e) {
		q.offer(e);
	}

	@Override
	public Iterator<E> iterator() {
		return q.iterator();
	}

}
