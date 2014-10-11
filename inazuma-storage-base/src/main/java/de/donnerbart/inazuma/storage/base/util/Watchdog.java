package de.donnerbart.inazuma.storage.base.util;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Watchdog extends Thread
{
	private final Callable<Boolean> callable;
	private final long timeout;
	private final long delay;
	private final CountDownLatch countDownLatch;

	private volatile boolean keepRunning = true;

	public static void watchdog(final Callable<Boolean> callable)
	{
		watchdog(callable, 10000, 250);
	}

	public static void watchdog(final Callable<Boolean> callable, final long timeout)
	{
		watchdog(callable, timeout, 250);
	}

	public static void watchdog(final Callable<Boolean> callable, final long timeout, final long delay)
	{
		new Watchdog(callable, timeout, delay).start();
	}

	public Watchdog(final Callable<Boolean> callable, final long timeout, final long delay)
	{
		this.callable = callable;
		this.timeout = timeout;
		this.delay = delay;
		this.countDownLatch = new CountDownLatch(1);
	}

	@Override
	public synchronized void start()
	{
		super.start();
		try
		{
			if (timeout > 0)
			{
				countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
			}
			else
			{
				countDownLatch.await();
			}
			this.interruptAndJoin();
		}
		catch (InterruptedException ignored)
		{
		}
	}

	public void interruptAndJoin()
	{
		super.interrupt();
		keepRunning = false;
		try
		{
			// Make sure to wait for the other thread to die before continuing
			join(timeout);
		}
		catch (InterruptedException ignored)
		{
		}
	}

	@Override
	public void run()
	{
		while (keepRunning)
		{
			try
			{
				if (callable.call())
				{
					keepRunning = false;
					countDownLatch.countDown();
				}
				else
				{
					TimeUnit.MILLISECONDS.sleep(delay);
				}
			}
			catch (Exception ignored)
			{
			}
		}
	}
}