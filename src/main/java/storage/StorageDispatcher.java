package storage;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import storage.message.BaseMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class StorageDispatcher extends UntypedActor
{
	private final StorageController storageController;

	private final ConcurrentMap<String, ActorRef> storageProcessorByUserID = new ConcurrentHashMap<>();

	public StorageDispatcher(final StorageController storageController)
	{
		this.storageController = storageController;
	}

	@Override
	public void onReceive(Object message) throws Exception
	{
		if (message instanceof BaseMessage)
		{
			final BaseMessage baseMessage = (BaseMessage) message;
			switch (baseMessage.getType())
			{
				case STORAGE_PROCESSOR_IDLE:
				{
					storageProcessorByUserID.remove(baseMessage.getUserID());
					sender().tell(PoisonPill.getInstance(), self());
					break;
				}
				case DELETE_DOCUMENT:
				case FETCH_DOCUMENT:
				case FETCH_DOCUMENT_METADATA:
				case LOAD_DOCUMENT_METADATA:
				case ADD_DOCUMENT:
				case PERSIST_DOCUMENT_METADATA:
				{
					findOrCreateProcessorFor(baseMessage.getUserID()).tell(message, self());
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
}
