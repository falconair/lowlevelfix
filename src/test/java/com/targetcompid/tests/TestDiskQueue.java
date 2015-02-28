package com.targetcompid.tests;

import org.junit.Test;
import static java.lang.System.out;

import com.targetcompid.util.DiskQueue;

public class TestDiskQueue {

	@Test
	public void basicTest() throws Exception{
		DiskQueue d = DiskQueue.getInstance("test.fix");
		d.offer("hello world");
		d.offer("hello world");
		d.close();
		
		DiskQueue e = DiskQueue.getInstance("test.fix");
		
		out.println("BEFORE========");
		
		for(String s : e){
			out.println(s);
		}
		
		e.offer("world hello");
		e.offer("world hello");
		
		out.println("AFTER========");
		
		for(String s : e){
			out.println(s);
		}
		e.close();
	}
	
	@Test
	public void duplicateTest() throws Exception{
			DiskQueue d = DiskQueue.getInstance("test.fix");
			d.offer("hello world");
			d.offer("hello world");
			d.offer("hello world");
			d.offer("hello world");
			d.offer("hello world");
			d.offer("hello world");
			d.offer("hello world");
			d.offer("hello world");
			//d.close();
			
			DiskQueue e = DiskQueue.getInstance("test.fix");
			e.offer("world hello");
			e.offer("world hello");
			e.offer("world hello");
			e.offer("world hello");
			e.offer("world hello");
			e.offer("world hello");
			e.offer("world hello");
			
			d.offer("world hello");

	}
}
