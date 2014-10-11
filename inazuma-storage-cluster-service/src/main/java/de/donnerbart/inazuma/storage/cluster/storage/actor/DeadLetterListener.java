package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.DeadLetter;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.dispatch.sysmsg.Terminate;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ControlMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.user.UserMessage;

class DeadLetterListener extends UntypedActor
{
	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	@Override
	public void postStop() throws Exception
	{
		super.postStop();
		System.out.println("    The DeadLetterListener has stopped!");
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof DeadLetter)
		{
			final DeadLetter deadLetter = ((DeadLetter) message);
			final Object deadMessage = deadLetter.message();
			if (!(deadMessage instanceof Terminate) && !(deadMessage instanceof PoisonPill))
			{
				if (deadMessage instanceof ControlMessage)
				{
					log.error("Received control message of type {} with content {}", deadMessage.getClass(), deadLetter.recipient());

					return;
				}
				else if (deadMessage instanceof UserMessage)
				{
					final UserMessage userMessage = (UserMessage) deadMessage;
					log.error("Received base message of type {} with content {} for actor {}", userMessage.getClass(), userMessage.getUserID(), deadLetter.recipient());

					return;
				}

				log.error("Received dead letter {} for actor {}", deadLetter.message(), deadLetter.recipient());
			}
		}
	}
}
