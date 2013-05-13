Low Level FIX
=============

This project aims to provide very low level modules which can be combined to produce a FIX protocol handler (server, client, router, etc.).

Consider the current implementation, at best, late alpha quality.  There is almost no documentation, optimizations, etc.  This is little more than a code dump at this point.

See various tests for examples on using this code.

===Acceptor API usage===

final int port = 5555;
		
FIXAcceptor server = FIXAcceptor.Builder(port).withDebugStatus(true).build();
server.startListening();
		
final Map<String,String> msg = new LinkedHashMap<String,String>();
//...test request or some other msg
server.sendMsg("SENDER", msg);

===Initiator API usage===

FIXInitiator client = FIXInitiator.Builder("FIX.4.2", "CLIENT", "SERVER", "localhost", 5555).withDebugStatus(true).build();

client.onMsg(new IMessageCallback() {
			
	@Override
	public void onMsg(Map<String, String> msg) {
		out.println(msg);
	}
			
	@Override
	public void onException(Throwable t) {
		t.printStackTrace();
	}
});
		
client.logOn();
		
		

===TODO===
-Make sure netty handlers are sane (ignore FIX logic for now)
-Document (javadocs)
-Make sure existing logic works correctly (add more standard FIX test cases)
-Add new functionality such as groups, crash proof resend logic, etc.

Contributions welcome.

shahbazc gmail com

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/1c567e793871417659e0c119d1b6dfde "githalytics.com")](http://githalytics.com/falconair/lowlevelfix)
