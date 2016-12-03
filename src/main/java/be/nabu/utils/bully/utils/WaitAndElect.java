package be.nabu.utils.bully.utils;

import be.nabu.utils.bully.BullyClient;

public class WaitAndElect implements Runnable {

	private BullyClient client;
	private long timeout;

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
			client.elect();
		}
	}
}
