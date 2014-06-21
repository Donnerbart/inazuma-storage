package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.Terminated;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessageType;

public class TheReaper extends UntypedActor
{
	private final BlockingCallback<Object> callbackAllSoulsReaped;
	private final BlockingCallback<Integer> callbackReportWatchedActorCount;

	private int watchCounter;

	public TheReaper(final BlockingCallback<Object> callbackAllSoulsReaped, final BlockingCallback<Integer> callbackReportWatchedActorCount)
	{
		this.callbackAllSoulsReaped = callbackAllSoulsReaped;
		this.callbackReportWatchedActorCount = callbackReportWatchedActorCount;
		this.watchCounter = 0;
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message == ControlMessageType.WATCH_ME)
		{
			++watchCounter;
			context().watch(sender());
		}
		else if (message instanceof Terminated)
		{
			if (--watchCounter == 0)
			{
				allSoulsReaped();
			}
		}
		else if (message == ControlMessageType.REPORT_WATCH_COUNT)
		{
			callbackReportWatchedActorCount.setResult(watchCounter);
		}
		else
		{
			unhandled(message);
		}
	}

	private void allSoulsReaped()
	{
		callbackAllSoulsReaped.setResult(null);

		context().system().shutdown();
	}
}
