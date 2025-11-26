/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import recipes_service.data.Operation;
//LSim logging system imports sgeag@2017
//import lsim.coordinator.LSimCoordinator;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	/**
	 * Creates an empty log.
	 * Initializes an empty operation list (Vector) for each participant.
	 * * @param participants The list of all replica IDs in the system.
	 */
	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * Inserts an operation into the log. 
	 * Operations are inserted in order based on their timestamp. 
	 * It rejects old/duplicate operations or operations that 
	 * arrive out of sequence (creating a gap).
	 * * @param op The operation to be added.
	 * @return true if op is successfully inserted, false otherwise (e.g., duplicate or out of order).
	 */
	public boolean add(Operation op){
		Timestamp opTS = op.getTimestamp();
		
		if (opTS.isNullTimestamp()) {
			 LSimLogger.log(Level.WARN, "Log.add: Received null timestamp operation.");
			 return false;
		}
		
		String replicaId = opTS.getHostid();
		List<Operation> opList = this.log.get(replicaId);
		
		if (opList == null) {
			LSimLogger.log(Level.WARN, "Log.add: Received operation for unknown replicaId: " + replicaId);
			return false;
		}
		
		synchronized (opList) {
			if (opList.isEmpty()) {
				Timestamp zeroTS = new Timestamp(replicaId, 0); 
				
				if (opTS.compare(zeroTS) == 0) {
					opList.add(op);
					LSimLogger.log(Level.TRACE, "Log.add: Added first operation (seq 0) from " + replicaId);
					return true;
				} else {
					LSimLogger.log(Level.WARN, "Log.add: Expected seqNum 0 from " + replicaId + ", but got non-zero: " + opTS.toString());
					return false;
				}
			} else {
				Operation lastOp = opList.get(opList.size() - 1);
				Timestamp lastTS = lastOp.getTimestamp();
				
				long comparison = opTS.compare(lastTS);
				
				if (comparison == 1) {
					opList.add(op);
					LSimLogger.log(Level.TRACE, "Log.add: Added sequential operation from " + replicaId + ": " + opTS.toString());
					return true;
				} else if (comparison <= 0) {

					LSimLogger.log(Level.TRACE, "Log.add: Ignoring old/duplicate operation from " + replicaId + ": " + opTS.toString());
					return false; 
				} else {
					LSimLogger.log(Level.WARN, "Log.add: Sequence gap. Expected op after " + lastTS.toString() + " from " + replicaId + ", but got " + opTS.toString());
					return false;
				}
			}
		}
	}
	
	/**
	 * Compares this log against a remote summary vector (sum).
	 * It determines and returns all operations in this log that
	 * are "newer" than the timestamps present in the provided summary.
	 * * @param sum The TimestampVector (summary) from the remote replica.
	 * @return A list of operations that the remote replica has not seen.
	 */
	public List<Operation> listNewer(TimestampVector sum){

		List<Operation> newerOps = new Vector<Operation>();
		
		for (String replicaId : this.log.keySet()) {
			List<Operation> opList = this.log.get(replicaId);
			
			
			Timestamp lastSeenByOther = sum.getLast(replicaId);
			
			
			synchronized (opList) {
				
				for (Operation op : opList) {
					Timestamp opTS = op.getTimestamp();
					
					if (opTS.compare(lastSeenByOther) > 0) {
						newerOps.add(op);
					}
				}
			}
		}
		return newerOps;
	}
	
	/**
	 * Removes operations from the log that have been acknowledged 
	 * by all members of the group, according to the provided
	 * ackSummary. (Not implemented in Phase 1).
	 * * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
public boolean equals(Object obj) {
		
		if (obj == null) return false;
		if (obj == this) return true;
		if (!(obj instanceof Log)) return false;
		
		Log other = (Log) obj;
		return this.log.equals(other.log);
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}
