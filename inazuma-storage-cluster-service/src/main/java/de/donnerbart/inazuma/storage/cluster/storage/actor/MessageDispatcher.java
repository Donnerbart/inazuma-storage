package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.japi.pf.ReceiveBuilder;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessageType;
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

		theReaper.tell(ControlMessage.create(ControlMessageType.WATCH_ME), getSelf());
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof BaseMessage)
		{
			final BaseMessage baseMessage = (BaseMessage) message;
			processBaseMessage(baseMessage.getUserID()).tell(message, getSelf());
		}
		else if (message instanceof ControlMessage)
		{
			final ControlMessage controlMessage = (ControlMessage) message;
			switch (controlMessage.getType())
			{
				case REMOVE_IDLE_MESSAGE_PROCESSOR:
				{
					messageProcessorByUserID.remove(controlMessage.getContent());
					sender().tell(PoisonPill.getInstance(), getSelf());
					break;
				}
				case SHUTDOWN:
				{
					processShutdown();
					break;
				}
				default:
				{
					unhandled(controlMessage);
				}
			}
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
		theReaper.tell(ControlMessage.create(ControlMessageType.WATCH_ME), messageProcessor);

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
