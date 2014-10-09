package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.japi.pf.ReceiveBuilder;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.RemoveIdleMessageProcessorMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ShutdownMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.WatchMeMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.user.UserMessage;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.HashMap;
import java.util.Map;

class MessageDispatcher extends UntypedActor
{
	private static final PartialFunction<Object, BoxedUnit> shutdownBehaviour = ReceiveBuilder.matchAny(null).build();

	private final StorageController storageController;
	private final ActorRef theReaper;

	private final Map<String, ActorRef> messageProcessorByUserID = new HashMap<>();

	public MessageDispatcher(final StorageController storageController, final ActorRef theReaper)
	{
		this.storageController = storageController;
		this.theReaper = theReaper;

		theReaper.tell(WatchMeMessage.getInstance(), getSelf());
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof UserMessage)
		{
			final UserMessage userMessage = (UserMessage) message;
			processBaseMessage(userMessage.getUserID()).tell(message, getSelf());
		}
		else if (message instanceof RemoveIdleMessageProcessorMessage)
		{
			messageProcessorByUserID.remove(((RemoveIdleMessageProcessorMessage) message).getUserID());
			sender().tell(PoisonPill.getInstance(), getSelf());
		}
		else if (message instanceof ShutdownMessage)
		{
			processShutdown();
		}
		else
		{
			unhandled(message);
		}
	}

	private ActorRef processBaseMessage(final String userID)
	{
		final ActorRef maybeActor = messageProcessorByUserID.get(userID);
		if (maybeActor != null)
		{
			return maybeActor;
		}

		final ActorRef messageProcessor = ActorFactory.createMessageProcessor(getContext(), storageController, userID);
		theReaper.tell(WatchMeMessage.getInstance(), messageProcessor);

		final ActorRef previousActor = messageProcessorByUserID.putIfAbsent(userID, messageProcessor);
		if (previousActor != null)
		{
			messageProcessor.tell(PoisonPill.getInstance(), getSelf());

			return previousActor;
		}

		return messageProcessor;
	}

	private void processShutdown()
	{
		getContext().become(shutdownBehaviour, true);

		for (final String userID : messageProcessorByUserID.keySet())
		{
			final ActorRef messageProcessor = messageProcessorByUserID.get(userID);
			messageProcessor.tell(PoisonPill.getInstance(), getSelf());
		}

		getSelf().tell(PoisonPill.getInstance(), getSelf());
	}
}
