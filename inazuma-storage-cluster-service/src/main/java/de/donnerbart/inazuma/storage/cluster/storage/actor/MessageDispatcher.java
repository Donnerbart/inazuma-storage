package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseCallbackMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseMessage;

import java.util.HashMap;
import java.util.Map;

public class MessageDispatcher extends UntypedActor
{
	private final StorageController storageController;

	private final Map<String, ActorRef> messageProcessorByUserID = new HashMap<>();

	private boolean running = true;

	public MessageDispatcher(final StorageController storageController)
	{
		this.storageController = storageController;
	}

	@Override
	public void postStop()
	{
		storageController.shutdownCountdown();
	}

	@Override
	public void onReceive(Object message) throws Exception
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
				case STORAGE_PROCESSOR_IDLE:
				{
					messageProcessorByUserID.remove(baseMessage.getUserID());
					sender().tell(PoisonPill.getInstance(), self());
					break;
				}
				case SHUTDOWN:
				{
					initShutdown(baseMessage);
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

	private ActorRef findOrCreateProcessorFor(final String userID)
	{
		final ActorRef maybeActor = messageProcessorByUserID.get(userID);
		if (maybeActor != null)
		{
			return maybeActor;
		}

		final ActorRef messageProcessor = ActorFactory.createMessageProcessor(context(), storageController, userID);
		final ActorRef previousActor = messageProcessorByUserID.putIfAbsent(userID, messageProcessor);
		if (previousActor != null)
		{
			messageProcessor.tell(PoisonPill.getInstance(), self());

			return previousActor;
		}

		return messageProcessor;
	}

	@SuppressWarnings("unchecked")
	private void initShutdown(final BaseMessage message)
	{
		running = false;

		storageController.setShutdownCountdown(messageProcessorByUserID.size() + 1);
		((BaseCallbackMessage<Object>) message).getCallback().setResult(null);

		for (final String userID : messageProcessorByUserID.keySet())
		{
			final ActorRef messageProcessor = messageProcessorByUserID.get(userID);
			messageProcessor.tell(PoisonPill.getInstance(), self());
		}

		self().tell(PoisonPill.getInstance(), self());
	}
}
