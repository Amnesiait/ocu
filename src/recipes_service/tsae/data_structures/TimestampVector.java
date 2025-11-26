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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{
	// Only for the zip file with the correct solution of phase1.Needed for the logging system for the phase1. sgeag_2018p 
//	private transient LSimCoordinator lsim = LSimFactory.getCoordinatorInstance();
	// Needed for the logging system sgeag@2017
//	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();
	
	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	/**
	 * Creates an empty TimestampVector.
	 * Initializes the vector with a null timestamp 
	 * (seq number < 0) for each participant.
	 * * @param participants The list of all replica IDs in the system.
	 */
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the vector with a new timestamp.
	 * The update only occurs if the new timestamp is strictly
	 * newer than the one currently stored for that timestamp's host.
	 * * @param timestamp The new timestamp to process.
	 */
	public void updateTimestamp(Timestamp timestamp){
		LSimLogger.log(Level.TRACE, "Updating the TimestampVectorInserting with the timestamp: "+timestamp);

		String replicaId = timestamp.getHostid(); 
		Timestamp current = this.timestampVector.get(replicaId);
		
		if (current == null || timestamp.compare(current) > 0) {
			this.timestampVector.put(replicaId, timestamp);
		}
	}
	
	/**
	 * Merges this vector with another vector (tsVector) by taking 
	 * the element-wise maximum of the timestamps for each host.
	 * * @param tsVector The other TimestampVector to merge with.
	 */
	public void updateMax(TimestampVector tsVector){

		for (String replicaId : this.timestampVector.keySet()) {
			Timestamp myTS = this.timestampVector.get(replicaId);
			Timestamp otherTS = tsVector.getLast(replicaId);
			
			if (otherTS != null) {
	
				if (myTS.compare(otherTS) < 0) {
					this.timestampVector.put(replicaId, otherTS);
				}
			}
		}
	}
	
	/**
	 * Gets the last (most recent) timestamp stored for a specific node.
	 * * @param node The host ID of the replica.
	 * @return The corresponding Timestamp, or null if the node is not in the vector.
	 */
	public Timestamp getLast(String node){
		
		return this.timestampVector.get(node);
	}
	
	/**
	 * Merges this vector with another vector (tsVector) by taking
	 * the element-wise minimum of the timestamps for each host.
	 * * @param tsVector The other TimestampVector to merge with.
	 */
	public void mergeMin(TimestampVector tsVector){
		
		for (String replicaId : this.timestampVector.keySet()) {
			Timestamp myTS = this.timestampVector.get(replicaId);
			Timestamp otherTS = tsVector.getLast(replicaId);

			if (otherTS != null) {

				if (myTS.isNullTimestamp()) {
					
					this.timestampVector.put(replicaId, otherTS);
				} 

				else if (!otherTS.isNullTimestamp()) {

					if (otherTS.compare(myTS) < 0) {
						this.timestampVector.put(replicaId, otherTS);
					}
				}
			}
		}
	}
	
	/**
	 * Creates a deep copy (clone) of this TimestampVector.
	 * The new vector will have the same participants and
	 * a copy of all current timestamps.
	 * * @return A new TimestampVector instance with the same state as this one.
	 */
	public TimestampVector clone(){
		
		List<String> participants = new java.util.ArrayList<String>(this.timestampVector.keySet());
		
		TimestampVector newVector = new TimestampVector(participants);
		

		for (String replicaId : this.timestampVector.keySet()) {
			Timestamp myTS = this.timestampVector.get(replicaId);
			newVector.timestampVector.put(replicaId, myTS);
		}
		return newVector;
	}
	
	/**
	 * equals
	 */
	public boolean equals(Object obj){
		
		if (obj == null) return false;
		if (obj == this) return true;
		if (!(obj instanceof TimestampVector)) return false;
		
		TimestampVector other = (TimestampVector) obj;
		
		return this.timestampVector.equals(other.timestampVector);
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}
}
