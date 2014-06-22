package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.ControlMessageType;

import java.util.HashMap;
import java.util.Map;

class MessageDispatcher extends UntypedActor
{
	private final StorageController storageController;
	private final ActorRef theReaper;

	private final Map<String, ActorRef> messageProcessorByUserID = new HashMap<>();

	private boolean running = true;

	public MessageDispatcher(final StorageController storageController, final ActorRef theReaper)
	{
		this.storageController = storageController;
		this.theReaper = theReaper;

		theReaper.tell(ControlMessage.create(ControlMessageType.WATCH_ME), self());
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof BaseMessage)
		{
			if (!running)
			{
				unhandled(message);

				return;
			}

			final BaseMessage baseMessage = (BaseMessage) message;
			processBaseMessage(baseMessage.getUserID()).tell(message, self());
		}
		else if (message instanceof ControlMessage)
		{
			final ControlMessage controlMessage = (ControlMessage) message;
			switch (controlMessage.getType())
			{
				case REMOVE_IDLE_MESSAGE_PROCESSOR:
				{
					messageProcessorByUserID.remove(controlMessage.getContent());
					sender().tell(PoisonPill.getInstance(), self());
					break;
				}
				case SHUTDOWN:
				{
					processShutdown();
					break;
				}
				default:
				{
					unhandled(message);
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

		final ActorRef messageProcessor = ActorFactory.createMessageProcessor(context(), storageController, userID);
		theReaper.tell(ControlMessage.create(ControlMessageType.WATCH_ME), messageProcessor);

		final ActorRef previousActor = messageProcessorByUserID.putIfAbsent(userID, messageProcessor);
		if (previousActor != null)
		{
			messageProcessor.tell(PoisonPill.getInstance(), self());

			return previousActor;
		}

		return messageProcessor;
	}

	private void processShutdown()
	{
		running = false;

		for (final String userID : messageProcessorByUserID.keySet())
		{
			final ActorRef messageProcessor = messageProcessorByUserID.get(userID);
			messageProcessor.tell(PoisonPill.getInstance(), self());
		}

		self().tell(PoisonPill.getInstance(), self());
	}
}
