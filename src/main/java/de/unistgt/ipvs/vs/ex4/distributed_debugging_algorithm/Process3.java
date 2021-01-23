package de.unistgt.ipvs.vs.ex4.distributed_debugging_algorithm;

//you are not allowed to change this class structure
public class Process3 extends AbstractProcess {
	private VectorClock vectorClock;

	public Process3(Monitor monitor, AbstractProcess[] processes, int Id) {
		super(monitor, processes, Id);

		vectorClock = new VectorClock(this.Id, this.numberOfProcesses);
	}

	@Override
	public void run() {

		//TODO Implement processes3 (Listing 4) code here!
		//Notify the monitor
		Message message = new Message(new VectorClock(vectorClock), this.localVariable);
		monitor.receiveMessage(this.Id, message);

		//Line 1
		this.localVariable = 4;
		this.vectorClock.increment();
		//Notify the monitor
		message = new Message(new VectorClock(vectorClock), this.localVariable);
		monitor.receiveMessage(this.Id, message);

		//Line 2
		this.localVariable *= 2;
		this.vectorClock.increment();
		//Notify the monitor
		message = new Message(new VectorClock(vectorClock), this.localVariable);
		monitor.receiveMessage(this.Id, message);

		//Line 3
		Message receivedMessage = receive(0); // receive from process 1
		this.vectorClock.update(receivedMessage.getVectorClock());
		this.localVariable = receivedMessage.getLocalVariable() - this.localVariable;
		this.vectorClock.increment();

		//Notify the monitor
		message = new Message(new VectorClock(vectorClock), this.localVariable);
		monitor.receiveMessage(this.Id, message);

		//Line 4
		this.localVariable -= 2;
		this.vectorClock.increment();
		//Notify the monitor
		message = new Message(new VectorClock(vectorClock), this.localVariable);
		monitor.receiveMessage(this.Id, message);

		//Line 5
		this.localVariable += 11;
		this.vectorClock.increment();
		//Notify the monitor
		message = new Message(new VectorClock(vectorClock), this.localVariable);
		monitor.receiveMessage(this.Id, message);


		//Line 6
		send(0, message);


		//send terminate signal
		monitor.processTerminated(this.Id);
		System.out.printf("process:%d , the local variable= %d\n", this.Id, this.localVariable);


	}

}
