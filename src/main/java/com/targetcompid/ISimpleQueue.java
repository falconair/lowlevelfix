package com.targetcompid;


public interface ISimpleQueue<E> extends Iterable<E>{

	public void offer(E e) throws Exception;
}
