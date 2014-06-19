/**
 * 
 * Copyright 2014 Central Software

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.central.varth.resp.cluster;

public class ClusterInfo {

	private String state;
	private int slotsAssigned;
	private int slotsOk;
	private int slotsPfail;
	private int slotsFail;
	private int knownNodes;
	private int size;
	private int currentEpoch;
	private int statsMessageSent;
	private int statsMessageReceived;
	
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public int getSlotsAssigned() {
		return slotsAssigned;
	}
	public void setSlotsAssigned(int slotsAssigned) {
		this.slotsAssigned = slotsAssigned;
	}
	public int getSlotsOk() {
		return slotsOk;
	}
	public void setSlotsOk(int slotsOk) {
		this.slotsOk = slotsOk;
	}
	public int getSlotsPfail() {
		return slotsPfail;
	}
	public void setSlotsPfail(int slotsPfail) {
		this.slotsPfail = slotsPfail;
	}
	public int getSlotsFail() {
		return slotsFail;
	}
	public void setSlotsFail(int slotsFail) {
		this.slotsFail = slotsFail;
	}
	public int getKnownNodes() {
		return knownNodes;
	}
	public void setKnownNodes(int knownNodes) {
		this.knownNodes = knownNodes;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getCurrentEpoch() {
		return currentEpoch;
	}
	public void setCurrentEpoch(int currentEpoch) {
		this.currentEpoch = currentEpoch;
	}
	public int getStatsMessageSent() {
		return statsMessageSent;
	}
	public void setStatsMessageSent(int statsMessageSent) {
		this.statsMessageSent = statsMessageSent;
	}
	public int getStatsMessageReceived() {
		return statsMessageReceived;
	}
	public void setStatsMessageReceived(int statsMessageReceived) {
		this.statsMessageReceived = statsMessageReceived;
	}
	
}
