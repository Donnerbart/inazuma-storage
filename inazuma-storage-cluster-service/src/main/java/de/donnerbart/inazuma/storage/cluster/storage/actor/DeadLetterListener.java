package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.DeadLetter;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.dispatch.sysmsg.Terminate;
import akka.event.Logging;
import akka.event.LoggingAdapter;

class DeadLetterListener extends UntypedActor
{
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof DeadLetter)
		{
			final DeadLetter deadLetter = ((DeadLetter) message);
			final Object deadMessage = deadLetter.message();
			if (!(deadMessage instanceof Terminate) && !(deadMessage instanceof PoisonPill))
			{
				log.error("Received dead letter {} for actor {}", deadLetter.message(), deadLetter.recipient());
				throw new RuntimeException("Received dead letter " + deadLetter.message() + " for actor " + deadLetter.recipient());
			}
		}
	}
}
