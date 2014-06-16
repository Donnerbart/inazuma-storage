package de.donnerbart.inazuma.storage.client.main;

import de.donnerbart.inazuma.storage.client.inazuma.InazumaStorageClientManager;

import java.util.concurrent.CountDownLatch;

public class Main
{
	public static void main(final String[] args)
	{
		final CountDownLatch latch = InazumaStorageClientManager.start();

		// Wait for shutdown hook
		System.out.println("Inazuma-Storage-Client is running...");
		try
		{
			latch.await();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		System.out.println("Inazuma-Storage-Client is shut down!");
		System.exit(0);
	}
}
