package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.Terminated;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessageType;

public class TheReaper extends UntypedActor
{
	private final BlockingCallback<Object> callback;

	private int watchCounter;

	public TheReaper(final BlockingCallback<Object> callback)
	{
		this.callback = callback;
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
		else
		{
			unhandled(message);
		}
	}

	private void allSoulsReaped()
	{
		callback.setResult(null);

		context().system().shutdown();
	}
}
