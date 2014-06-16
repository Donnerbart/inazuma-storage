package de.donnerbart.inazuma.storage.cluster.storage;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseCallbackMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.BaseMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class StorageDispatcher extends UntypedActor
{
	private final StorageController storageController;

	private final ConcurrentMap<String, ActorRef> storageProcessorByUserID = new ConcurrentHashMap<>();

	private boolean running = true;

	public StorageDispatcher(final StorageController storageController)
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
					storageProcessorByUserID.remove(baseMessage.getUserID());
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
		final ActorRef maybeActor = storageProcessorByUserID.get(userID);
		if (maybeActor != null)
		{
			return maybeActor;
		}

		final ActorRef storageProcessor = StorageActorFactory.createStorageProcessor(context(), storageController, userID);
		final ActorRef previousActor = storageProcessorByUserID.putIfAbsent(userID, storageProcessor);

		return (previousActor != null) ? previousActor : storageProcessor;
	}

	@SuppressWarnings("unchecked")
	private void initShutdown(final BaseMessage message)
	{
		running = false;

		storageController.setShutdownCountdown(storageProcessorByUserID.size() + 1);
		((BaseCallbackMessage<Object>) message).getCallback().setResult(null);

		for (final String userID : storageProcessorByUserID.keySet())
		{
			final ActorRef storageProcessor = storageProcessorByUserID.get(userID);
			storageProcessor.tell(PoisonPill.getInstance(), self());
		}

		self().tell(PoisonPill.getInstance(), self());
	}
}
