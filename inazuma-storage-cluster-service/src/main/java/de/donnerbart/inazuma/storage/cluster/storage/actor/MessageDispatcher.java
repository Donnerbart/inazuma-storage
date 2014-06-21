package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseMessage;
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

		theReaper.tell(ControlMessageType.WATCH_ME, self());
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof BaseMessage && running)
		{
			final BaseMessage baseMessage = (BaseMessage) message;
			switch (baseMessage.getType())
			{
				case ADD_DOCUMENT:
				case FETCH_DOCUMENT:
				case DELETE_DOCUMENT:
				case LOAD_DOCUMENT_METADATA:
				case FETCH_DOCUMENT_METADATA:
				case PERSIST_DOCUMENT_METADATA:
				{
					findOrCreateProcessorFor(baseMessage.getUserID()).tell(message, self());
					break;
				}
				case MESSAGE_PROCESSOR_IDLE:
				{
					messageProcessorByUserID.remove(baseMessage.getUserID());
					sender().tell(PoisonPill.getInstance(), self());
					break;
				}
				default:
				{
					unhandled(message);
				}
			}
		}
		else if (message == ControlMessageType.SHUTDOWN)
		{
			initShutdown();
		}
		else
		{
			unhandled(message);
		}
	}

	private ActorRef findOrCreateProcessorFor(final String userID)
	{
		final ActorRef maybeActor = messageProcessorByUserID.get(userID);
		if (maybeActor != null)
		{
			return maybeActor;
		}

		final ActorRef messageProcessor = ActorFactory.createMessageProcessor(context(), storageController, userID);
		theReaper.tell(ControlMessageType.WATCH_ME, messageProcessor);

		final ActorRef previousActor = messageProcessorByUserID.putIfAbsent(userID, messageProcessor);
		if (previousActor != null)
		{
			messageProcessor.tell(PoisonPill.getInstance(), self());

			return previousActor;
		}

		return messageProcessor;
	}

	private void initShutdown()
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
