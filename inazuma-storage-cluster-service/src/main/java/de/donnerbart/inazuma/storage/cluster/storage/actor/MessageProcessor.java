package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.cluster.storage.StorageControllerInternalFacade;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MessageProcessor extends UntypedActor
{
	private static final long DELAY = 50;
	private static final TimeUnit DELAY_UNIT = TimeUnit.MILLISECONDS;

	private final StorageControllerInternalFacade storageController;
	private final String userID;

	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private boolean isReady = false;
	private boolean persistDocumentMetadataMessageInQueue = false;
	private Map<String, DocumentMetadata> documentMetadataMap = new HashMap<>();

	public MessageProcessor(final StorageControllerInternalFacade storageController, final String userID)
	{
		this.storageController = storageController;
		this.userID = userID;

		processLoadDocumentMetadataMessage(new BaseMessage(MessageType.LOAD_DOCUMENT_METADATA, userID));
	}

	@Override
	public void preStart() throws Exception
	{
		context().setReceiveTimeout(Duration.create(5, TimeUnit.MINUTES));
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof ReceiveTimeout)
		{
			processReceivedTimeout();
			return;
		}

		final BaseMessage baseMessage = (message instanceof BaseMessage) ? (BaseMessage) message : null;
		if (baseMessage != null && baseMessage.getType() == MessageType.LOAD_DOCUMENT_METADATA)
		{
			processLoadDocumentMetadataMessage(baseMessage);
			return;
		}
		else if (!isReady)
		{
			sendDelayedMessage(message);
			return;
		}

		if (baseMessage != null)
		{
			switch (baseMessage.getType())
			{
				case PERSIST_DOCUMENT_METADATA:
				{
					processPersistDocumentMetadata(baseMessage);
					break;
				}
				case ADD_DOCUMENT:
				{
					processPersistDocument((AddDocumentMessage) message);
					break;
				}
				case DELETE_DOCUMENT:
				{
					processDeleteDocument((BaseMessageWithKey) message);
					break;
				}
				case FETCH_DOCUMENT:
				{
					processFetchDocument((BaseMessageWithKey) message);
					break;
				}
				case FETCH_DOCUMENT_METADATA:
				{
					processFetchDocumentMetadata(baseMessage);
					break;
				}
				case MARK_DOCUMENT_AS_READ:
				{
					processMarkDocumentAsRead((BaseMessageWithKey) baseMessage);
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

	private void sendDelayedMessage(final Object message)
	{
		context().system().scheduler().scheduleOnce(
				Duration.create(DELAY, DELAY_UNIT),
				self(),
				message,
				context().system().dispatcher(),
				self()
		);
	}

	private void sendPersistDocumentMetadataMessage()
	{
		self().tell(new BaseMessage(MessageType.PERSIST_DOCUMENT_METADATA, userID), getSelf());
	}

	private void processReceivedTimeout()
	{
		isReady = false;
		documentMetadataMap.clear();

		context().parent().tell(new BaseMessage(MessageType.MESSAGE_PROCESSOR_IDLE, userID), self());
	}

	private void processLoadDocumentMetadataMessage(final BaseMessage message)
	{
		try
		{
			final String documentMetadataJSON = storageController.getCouchbaseWrapper().getUserDocumentMetadata(userID);
			if (documentMetadataJSON != null)
			{
				documentMetadataMap = GsonWrapper.getDocumentMetadataMap(documentMetadataJSON);
				if (documentMetadataMap == null)
				{
					throw new RuntimeException("Document metadata for user " + userID + " is null! " + documentMetadataJSON);
				}
			}

			isReady = true;
		}
		catch (Exception e)
		{
			log.debug("Could not create document metadata for user {}: {}", userID, e.getMessage());

			sendDelayedMessage(message);
		}
	}

	private void processPersistDocumentMetadata(final BaseMessage message)
	{
		persistDocumentMetadataMessageInQueue = false;

		try
		{
			storageController.getCouchbaseWrapper().storeDocumentMetadata(userID, GsonWrapper.toJson(documentMetadataMap));
		}
		catch (Exception e)
		{
			log.debug("Could not store document metadata for user {}: {}", userID, e.getMessage());

			storageController.incrementMetadataRetries();
			sendDelayedMessage(message);

			return;
		}

		storageController.incrementMetadataPersisted();
	}

	@SuppressWarnings("unchecked")
	private void processFetchDocumentMetadata(final BaseMessage message)
	{
		((BaseCallbackMessage<String>) message).setResult(GsonWrapper.toJson(documentMetadataMap));
	}

	private void processPersistDocument(final AddDocumentMessage message)
	{
		try
		{
			storageController.getCouchbaseWrapper().storeDocument(message.getKey(), message.getJson());
		}
		catch (Exception e)
		{
			log.debug("Could not add {} for user {}: {}", message.getKey(), userID, e.getMessage());

			storageController.incrementDocumentRetries();
			sendDelayedMessage(message);

			return;
		}

		final DocumentMetadata documentMetadata = new DocumentMetadata(message);
		documentMetadataMap.put(message.getKey(), documentMetadata);

		if (!persistDocumentMetadataMessageInQueue)
		{
			persistDocumentMetadataMessageInQueue = true;

			storageController.incrementQueueSize();
			sendPersistDocumentMetadataMessage();
		}

		// We have to decrement the queue size AFTER we added a possible retry message
		// Otherwise the queueSize could drop to 0 and the system continues with shutdown
		storageController.incrementDocumentPersisted();
	}

	@SuppressWarnings("unchecked")
	private void processFetchDocument(final BaseMessageWithKey message)
	{
		final String document = storageController.getCouchbaseWrapper().getDocument(message.getKey());
		((BaseCallbackMessageWithKey<String>) message).setResult(document);

		storageController.incrementDocumentFetched();
	}

	private void processDeleteDocument(final BaseMessageWithKey message)
	{
		try
		{
			storageController.getCouchbaseWrapper().deleteDocument(message.getKey());
			documentMetadataMap.remove(message.getKey());

			sendPersistDocumentMetadataMessage();
		}
		catch (Exception e)
		{
			log.debug("Could not delete document {} for user {}: {}", message.getKey(), userID, e.getMessage());

			storageController.incrementDocumentRetries();
			sendDelayedMessage(message);

			return;
		}

		storageController.incrementDataDeleted();
	}

	private void processMarkDocumentAsRead(final BaseMessageWithKey baseMessage)
	{
		documentMetadataMap.get(baseMessage.getKey()).setRead(true);

		sendPersistDocumentMetadataMessage();
	}
}
