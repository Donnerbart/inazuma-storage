package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.DeadLetter;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.dispatch.sysmsg.Terminate;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessage;

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
				if (deadMessage instanceof ControlMessage)
				{
					final ControlMessage controlMessage = (ControlMessage) deadMessage;
					log.error("Received control message of type {} with content {} for actor {}", controlMessage.getType(), controlMessage.getContent(), deadLetter.recipient());

					return;

				}
				else if (deadMessage instanceof BaseMessage)
				{
					final BaseMessage baseMessage = (BaseMessage) deadMessage;
					log.error("Received base message of type {} with content {} for actor {}", baseMessage.getType(), baseMessage.getUserID(), deadLetter.recipient());

					return;
				}

				log.error("Received dead letter {} for actor {}", deadLetter.message(), deadLetter.recipient());
			}
		}
	}
}
