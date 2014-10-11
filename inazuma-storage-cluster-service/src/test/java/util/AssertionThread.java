package util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AssertionThread extends Thread
{
	private final Runnable runnable;
	private final long timeout;
	private final long delay;
	private final CountDownLatch countDownLatch;

	private volatile boolean keepRunning = true;
	private volatile AssertionError assertionError = null;

	public static void assertEventually(final Runnable runnable)
	{
		assertEventually(runnable, 2000, 250);
	}

	public static void assertEventually(final Runnable runnable, final long timeout)
	{
		assertEventually(runnable, timeout, 250);
	}

	public static void assertEventually(final Runnable runnable, final long timeout, final long delay)
	{
		new AssertionThread(runnable, timeout, delay).start();
	}

	public AssertionThread(final Runnable runnable, final long timeout, final long delay)
	{
		super(runnable);
		this.runnable = runnable;
		this.timeout = timeout;
		this.delay = delay;
		this.countDownLatch = new CountDownLatch(1);

		setUncaughtExceptionHandler((thread, exception) -> assertionError = (AssertionError) exception);
	}

	@Override
	public synchronized void start()
	{
		super.start();
		try
		{
			countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
			this.interruptAndJoin();
		}
		catch (InterruptedException ignored)
		{
		}
		if (assertionError != null)
		{
			throw assertionError;
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
				runnable.run();

				keepRunning = false;
				countDownLatch.countDown();
			}
			catch (AssertionError error)
			{
				try
				{
					TimeUnit.MILLISECONDS.sleep(delay);
				}
				catch (InterruptedException ignored)
				{
				}
			}
		}

		// Run the assertion one more time because the signal to stop may have come while the thread was sleeping
		if (countDownLatch.getCount() > 0)
		{
			runnable.run();
		}
	}
}