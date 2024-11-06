/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.utils.bully.utils;

import be.nabu.utils.bully.BullyClient;

public class WaitAndElect implements Runnable {

	private BullyClient client;
	private long timeout;
	private boolean isElecting;

	public WaitAndElect(BullyClient client, long timeout) {
		this.client = client;
		this.timeout = timeout;
	}

	@Override
	public void run() {
		boolean interrupted = false;
		if (timeout > 0) {
			try {
				Thread.sleep(timeout);
			}
			catch (InterruptedException e) {
				interrupted = true;
			}
		}
		if (!interrupted) {
			isElecting = true;
			client.elect();
		}
	}
	
	public boolean isElecting() {
		return isElecting;
	}
}
