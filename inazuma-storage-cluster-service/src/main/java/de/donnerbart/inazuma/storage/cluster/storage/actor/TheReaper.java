package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ReportWatchCountMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.WatchMeMessage;

class TheReaper extends UntypedActor
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
	public void postStop() throws Exception
	{
		super.postStop();
		System.out.println("TheReaper has stopped!");
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof Terminated)
		{
			if (--watchCounter == 0)
			{
				allSoulsReaped(((Terminated) message).getActor());
			}
		}
		else if (message instanceof WatchMeMessage)
		{
			++watchCounter;
			getContext().watch(sender());
		}
		else if (message instanceof ReportWatchCountMessage)
		{
			callbackReportWatchedActorCount.setResult(watchCounter);
		}
		else
		{
			unhandled(message);
		}
	}

	private void allSoulsReaped(final ActorRef actor)
	{
		callbackAllSoulsReaped.setResult(actor);

		getContext().system().shutdown();
	}
}
