package de.unistgt.ipvs.vs.ex4.distributed_debugging_algorithm;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

//you are not allowed to change this class structure. However, you can add local functions!
public class Monitor implements Runnable {

	/**
	 * The state consists on vector timestamp and local variables of each
	 * process. In this class, a state is represented by messages (events)
	 * indices of each process. The message contains a local variable and vector
	 * timestamp, see Message class. E.g. if state.processesMessagesCurrentIndex
	 * contains {1, 2}, it means that the state contains the second message
	 * (event) from process1 and the third message (event) from process2
	 */
	private class State {
		// Message indices of each process
		private int[] processesMessagesCurrentIndex;

		public State(int numberOfProcesses) {
			processesMessagesCurrentIndex = new int[numberOfProcesses];
		}

		public State(int[] processesMessagesCurrentIndex) {
			this.processesMessagesCurrentIndex = processesMessagesCurrentIndex;
		}

		{
			processesMessagesCurrentIndex = new int[numberOfProcesses];
		}

		public int[] getProcessesMessagesCurrentIndex() {
			return processesMessagesCurrentIndex;
		}

		public int getProcessMessageCurrentIndex(int processId) {
			return this.processesMessagesCurrentIndex[processId];
		}

		@Override
		public boolean equals(Object other) {
			State otherState = (State) other;

			// Iterate over processesMessagesCurrentIndex array
			for (int i = 0; i < numberOfProcesses; i++)
				if (this.processesMessagesCurrentIndex[i] != otherState.processesMessagesCurrentIndex[i])
					return false;

			return true;
		}
	}

	private int process_i_index = -1;
	private int process_j_index = -1;


	private int numberOfProcesses;
	private final int numberOfPredicates = 4;

	// Count of still running processes. The monitor starts to check predicates
	// (build lattice) whenever runningProcesses equals zero.
	private AtomicInteger runningProcesses;
	/*
	 * Q1, Q2, ..., Qn It represents the processes' queue. See distributed
	 * debugging algorithm from global state lecture!
	 */
	private List<List<Message>> processesMessages;

	// list of states
	private LinkedList<State> states;

	//Map of state vs level
	private Map<State, Integer> stateVsLevel;

	//Map pf level vs the nodes in that level.
	private Map<Integer, List<State>> levelVsStateList;

	// The predicates checking results
	private boolean[] possiblyTruePredicatesIndex;
	private boolean[] definitelyTruePredicatesIndex;

	public Monitor(int numberOfProcesses) {
		this.numberOfProcesses = numberOfProcesses;

		runningProcesses = new AtomicInteger();
		runningProcesses.set(numberOfProcesses);

		processesMessages = new ArrayList<>(numberOfProcesses);
		for (int i = 0; i < numberOfProcesses; i++) {
			List<Message> tempList = new ArrayList<>();
			processesMessages.add(i, tempList);
		}

		states = new LinkedList<>();

		possiblyTruePredicatesIndex = new boolean[numberOfPredicates];// there
																		// are
																		// three
		// predicates
		for (int i = 0; i < numberOfPredicates; i++)
			possiblyTruePredicatesIndex[i] = false;

		definitelyTruePredicatesIndex = new boolean[numberOfPredicates];
		for (int i = 0; i < numberOfPredicates; i++)
			definitelyTruePredicatesIndex[i] = false;
	}

	/**
	 * receive messages (events) from processes
	 *
	 * @param processId
	 * @param message
	 */
	public void receiveMessage(int processId, Message message) {
		synchronized (processesMessages) {
			//System.out.println(String.format("%d receved the message: %s:%s", processId, message.getLocalVariable(),
			//		message.getVectorClock().get()));
			processesMessages.get(processId).add(message);
		}
	}

	/**
	 * Whenever a process terminates, it notifies the Monitor. Monitor only
	 * starts to build lattice and check predicates when all processes terminate
	 *
	 * @param processId
	 */
	public void processTerminated(int processId) {
		runningProcesses.decrementAndGet();
	}

	public boolean[] getPossiblyTruePredicatesIndex() {
		return possiblyTruePredicatesIndex;
	}

	public boolean[] getDefinitelyTruePredicatesIndex() {
		return definitelyTruePredicatesIndex;
	}

	@Override
	public void run() {
		// wait till all processes terminate
		while (runningProcesses.get() != 0)
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		// create initial state (S00)
		State initialState = new State(numberOfProcesses);

		stateVsLevel = new HashMap<>();
		levelVsStateList = new HashMap<>();

		// check predicates for part (b)
		for (int predicateNo = 0; predicateNo < 3; predicateNo++) {
			System.out.printf("Predicate%d-----------------------------------\n",predicateNo);
			states.add(initialState); // add the initial state to states list
			buildLattice(predicateNo, 0, 1);
			states.clear();
		}

		if (numberOfProcesses > 2) {
			int predicateNo = 3;
			System.out.printf("Predicate%d-----------------------------------\n",predicateNo);
			states.add(initialState); // add the initial state to states list
			buildLattice(predicateNo, 0, 2);
			states.clear();
		}


	}

	public void buildLattice(int predicateNo, int process_i_id, int process_j_id) {
		// TODO
		/*
		 * - implement this function to build the lattice of consistent states.
		 * - The goal of building the lattice is to check a predicate if it is
		 * possibly or/and definitely True. Thus your function should stop
		 * whenever the predicate evaluates to both possibly and definitely
		 * True. NOTE1: this function should call findReachableStates and
		 * checkPredicate functions. NOTE2: predicateNo, process_i_id and
		 * process_j_id are described in checkPredicate function.
		 */
		process_i_index = process_i_id;
		process_j_index = process_j_id;

		findReachableStates(states.get(0));
		checkPredicate(predicateNo, process_i_id, process_j_id);

	}

	/**
	 * find all reachable states starting from a given state
	 *
	 * @param state
	 * @return list of all reachable states
	 */
	private LinkedList<State> findReachableStates(State state) {
		// TODO
		/*
		 * Given a state, implement a code that find all reachable states. The
		 * function should return a list of all reachable states
		 *
		 */

		LinkedList<State> reachableStates = new LinkedList<>();
		LinkedList<State> statesToCheck = new LinkedList<>();

		statesToCheck.add(state);

		stateVsLevel = new HashMap<>();
		levelVsStateList = new HashMap<>();

		stateVsLevel.put(state, 0);
		levelVsStateList.put(0, new ArrayList<>());

		while (!statesToCheck.isEmpty()) {
			LinkedList<State> statesInNextLevel = getStatesInNextLevel(statesToCheck.get(0));

			if(statesToCheck.get(0).equals(state)) {
				reachableStates.add(state);
			}

			//These data structures will be useful later on when checking for predicate.
			levelVsStateList.put(stateVsLevel.get(statesToCheck.get(0)) + 1, new ArrayList<>());

			for(State stateInNextLevel: statesInNextLevel) {
				stateVsLevel.put(stateInNextLevel, stateVsLevel.get(statesToCheck.get(0)) + 1);
				levelVsStateList.get(stateVsLevel.get(stateInNextLevel)).add(stateInNextLevel);
			}

			statesToCheck.remove();
			statesToCheck.addAll(statesInNextLevel);
		}

		return reachableStates;
	}

	/**
	 * Method to get the immediate next states given a state, note it
	 * only returns consistent states, i.e Vector clocks should be as expected.
	 * @param state
	 * @return
	 */
	private LinkedList<State> getStatesInNextLevel(State state) {
		/*Please note that since the ask here to construct a lattice with 2 process at max
		* I have done so, i.e we can either go left incrementing the state of the first process, or
		* we can go right incrementing the state of second process.Same can be done for three processes
		* If the ask was specified.*/
		int i = state.getProcessMessageCurrentIndex(process_i_index);
		int j = state.getProcessMessageCurrentIndex(process_j_index);

		LinkedList<State> reachableStates = new LinkedList<>();


		State thisState1 = new State(numberOfProcesses);
		thisState1.getProcessesMessagesCurrentIndex()[process_i_index] = i+1;
		thisState1.getProcessesMessagesCurrentIndex()[process_j_index] = j;

		//If the state of the first process is incremented.
		if(i+1 < processesMessages.get(process_i_index).size()) {
			VectorClock thisVectorClock = processesMessages.get(process_i_index).get(i + 1).getVectorClock();
			VectorClock otherVectorClock = processesMessages.get(process_j_index).get(j).getVectorClock();

			//Checking consistency of the state.
			if (thisVectorClock.checkConsistency(process_j_index, otherVectorClock)) {

				reachableStates.add(thisState1);
			}
		}

		State thisState2 = new State(numberOfProcesses);
		thisState2.getProcessesMessagesCurrentIndex()[process_i_index] = i;
		thisState2.getProcessesMessagesCurrentIndex()[process_j_index] = j+1;

		//If the state of the second process is incremented.
		if(j+1 < processesMessages.get(process_j_index).size()) {
			VectorClock thisVectorClock = processesMessages.get(process_i_index).get(i).getVectorClock();
			VectorClock otherVectorClock = processesMessages.get(process_j_index).get(j + 1).getVectorClock();

			//Checking consistency of the state.
			if (thisVectorClock.checkConsistency(process_j_index, otherVectorClock)) {
				reachableStates.add(thisState2);
			}
		}
		return reachableStates;

	}

	/**
	 * - check a predicate and return true if the predicate is **definitely**
	 * True. - To simplify the code, we check the predicates only on local
	 * variables of two processes. Therefore, process_i_Id and process_j_id
	 * refer to the processes that have the local variables in the predicate.
	 * The predicate0, predicate1 and predicate2 contain the local variables
	 * from process1 and process2. whilst the predicate3 contains the local
	 * variables from process1 and process3.
	 *
	 * @param predicateNo:
	 *            which predicate to validate
	 * @return true if predicate is definitely true else return false
	 */
	private boolean checkPredicate(int predicateNo, int process_i_Id, int process_j_id) {
		// TODO
		/*
		 * - check if a predicate is possibly and/or definitely true. - iterate
		 * over all reachable states to check the predicates. NOTE: you can use
		 * the following code switch (predicateNo) { case 0: predicate =
		 * Predicate.predicate0(process_i_Message, process_j_Message); break;
		 * case 1: ... }
		 */

		State startState = states.get(0);

		List<State> toTraverse = new ArrayList<>();
		toTraverse.add(startState);


		int level = 0;

		while (!toTraverse.isEmpty()) {
			// Going to next level compared to the node we are processing now.
			level++;


			//Get all the states in next level w.r.t to current set of nodes.
			List<State> nextStates = new ArrayList<>();
			while(!toTraverse.isEmpty()) {
				for (State st : (getStatesInNextLevel(toTraverse.get(0)))) {
					if(!nextStates.contains(st)) {
						nextStates.add(st);
					}
				}
				toTraverse.remove(0);
			}

			//Evaluate the predicate for ll the nodes.
			for(State state: nextStates) {
				boolean result=false;

				Message process_i_message = processesMessages.get(process_i_Id).
						get(state.getProcessMessageCurrentIndex(process_i_Id));
				Message process_j_message = processesMessages.get(process_j_id).
						get(state.getProcessMessageCurrentIndex(process_j_id));


				//System.out.print(String.format(" ( %s, %s) ", process_i_message.getLocalVariable(),
				//		process_j_message.getLocalVariable()));

				switch (predicateNo) {
					case 0:
						result=Predicate.predicate0(process_i_message, process_j_message);
						break;
					case 1:
						result=Predicate.predicate1(process_i_message, process_j_message);
						break;
					case 2:
						result=Predicate.predicate2(process_i_message, process_j_message);
						break;
					case 3:
						result=Predicate.predicate3(process_i_message, process_j_message);
						break;
				}

				if(result) {
					//In case predicate is true, it means it is possible to hit this node on way to final state
					// So possibly true is set to true for particular predicate.
					possiblyTruePredicatesIndex[predicateNo] = true;
				}
				else {
					//Since the predicate evaluated to true, we first check we have not already added this node
					// ad after that add this node for further processing.
					if(!toTraverse.contains(state)) {
						toTraverse.add(state);
					}
				}
			}

		}

		//Iff it has reached beyond the last level it means there is path where all nodes evaluate
		// to false, hence in that case alone definetly true is unsatisfied.
		definitelyTruePredicatesIndex[predicateNo] = level < levelVsStateList.keySet().size() - 1;

		return definitelyTruePredicatesIndex[predicateNo];


	}

}
