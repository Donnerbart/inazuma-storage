package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.cluster.storage.StorageControllerInternalFacade;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadataUtil;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import scala.concurrent.duration.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

class MessageProcessor extends UntypedActor
{
	private static final long DELAY = 50;
	private static final TimeUnit DELAY_UNIT = TimeUnit.MILLISECONDS;

	private final StorageControllerInternalFacade storageController;
	private final String userID;

	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private Map<String, DocumentMetadata> documentMetadataMap = null;
	private boolean persistDocumentMetadataMessageInQueue = false;

	public MessageProcessor(final StorageControllerInternalFacade storageController, final String userID)
	{
		this.storageController = storageController;
		this.userID = userID;

		processLoadDocumentMetadataMessage(ControlMessage.create(ControlMessageType.LOAD_DOCUMENT_METADATA));
	}

	@Override
	public void preStart() throws Exception
	{
		context().setReceiveTimeout(Duration.create(5, TimeUnit.MINUTES));
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		if (message instanceof BaseMessage)
		{
			if (documentMetadataMap == null)
			{
				sendDelayedMessage(message);

				return;
			}

			final BaseMessage baseMessage = (BaseMessage) message;
			switch (baseMessage.getType())
			{
				case FETCH_DOCUMENT:
				{
					processFetchDocument((BaseMessageWithKey) message);
					break;
				}
				case PERSIST_DOCUMENT:
				{
					processPersistDocument((AddDocumentMessage) message);
					break;
				}
				case DELETE_DOCUMENT:
				{
					processDeleteDocument((BaseMessageWithKey) message);
					break;
				}
				case MARK_DOCUMENT_AS_READ:
				{
					processMarkDocumentAsRead((BaseMessageWithKey) baseMessage);
					break;
				}
				case FETCH_DOCUMENT_METADATA:
				{
					processFetchDocumentMetadata(baseMessage);
					break;
				}
				case PERSIST_DOCUMENT_METADATA:
				{
					processPersistDocumentMetadata(baseMessage);
					break;
				}
				default:
				{
					unhandled(message);
				}
			}
		}
		else if (message instanceof ControlMessage)
		{
			final ControlMessage controlMessage = (ControlMessage) message;
			switch (controlMessage.getType())
			{
				case LOAD_DOCUMENT_METADATA:
				{
					processLoadDocumentMetadataMessage(controlMessage);
					break;
				}
				case CREATE_METADATA_DOCUMENT:
				{
					processCreateDocumentMetadataMessage(controlMessage);
					break;
				}
				case ADD_DOCUMENT_TO_METADATA:
				{
					processAddDocumentToMetadata((AddDocumentToMetadataControlMessage) controlMessage);
					break;
				}
				default:
				{
					unhandled(message);
				}
			}
		}
		else if (message instanceof ReceiveTimeout)
		{
			processReceivedTimeout();
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

	@SuppressWarnings("unchecked")
	private void processFetchDocument(final BaseMessageWithKey message)
	{
		storageController.getCouchbaseWrapper().getDocument(
				message.getKey()
		).doOnNext(document -> {
			if (!document.status().isSuccess())
			{
				log.debug("Could not load document for user {}: {}", userID, document);
				sendDelayedMessage(message);

				return;
			}

			((BaseCallbackMessageWithKey<String>) message).setResult(document.content());
			storageController.incrementDocumentFetched();
		}).subscribe();
	}

	private void processPersistDocument(final AddDocumentMessage message)
	{
		storageController.getCouchbaseWrapper().insertDocument(
				message.getKey(),
				message.getJson()
		).doOnNext(document -> {
			if (!document.status().isSuccess())
			{
				log.debug("Could not load document for user {}: {}", userID, document);
				sendDelayedMessage(message);
				storageController.incrementDocumentRetries();

				return;
			}

			final DocumentMetadata documentMetadata = new DocumentMetadata(message);
			self().tell(new AddDocumentToMetadataControlMessage(message.getKey(), documentMetadata), self());

		}).subscribe();
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
			sendDelayedMessage(message);
			storageController.incrementDocumentRetries();

			return;
		}

		storageController.incrementDataDeleted();
	}

	private void processMarkDocumentAsRead(final BaseMessageWithKey baseMessage)
	{
		documentMetadataMap.get(baseMessage.getKey()).setRead(true);

		sendPersistDocumentMetadataMessage();
	}

	@SuppressWarnings("unchecked")
	private void processFetchDocumentMetadata(final BaseMessage message)
	{
		((BaseCallbackMessage<String>) message).setResult(GsonWrapper.toJson(documentMetadataMap));
	}

	private void processPersistDocumentMetadata(final BaseMessage message)
	{
		persistDocumentMetadataMessageInQueue = false;

		storageController.getCouchbaseWrapper().insertDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID),
				GsonWrapper.toJson(documentMetadataMap)
		).doOnNext(document -> {
			if (!document.status().isSuccess())
			{
				log.debug("Could not store document metadata for user {}: {}", userID, document);
				sendDelayedMessage(message);
				storageController.incrementMetadataRetries();

				return;
			}

			storageController.incrementMetadataPersisted();

		}).subscribe();
	}

	private void processLoadDocumentMetadataMessage(final ControlMessage message)
	{
		storageController.getCouchbaseWrapper().getDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID)
		).doOnNext(document -> {
			if (!document.status().isSuccess())
			{
				log.debug("Could not load document metadata for user {}: {}", userID, document);
				sendDelayedMessage(message);

				return;
			}

			self().tell(ControlMessage.create(ControlMessageType.CREATE_METADATA_DOCUMENT, document.content()), self());
		}).subscribe();
	}

	private void processCreateDocumentMetadataMessage(final ControlMessage message)
	{
		documentMetadataMap = GsonWrapper.getDocumentMetadataMap(message.getContent());
		if (documentMetadataMap == null)
		{
			log.debug("Could not create document metadata for user {}: {}", userID, message.getContent());
			sendDelayedMessage(message);
		}
	}

	private void processAddDocumentToMetadata(final AddDocumentToMetadataControlMessage message)
	{
		documentMetadataMap.put(message.getContent(), message.getMetadata());

		if (!persistDocumentMetadataMessageInQueue)
		{
			persistDocumentMetadataMessageInQueue = true;

			storageController.incrementQueueSize();
			sendPersistDocumentMetadataMessage();
		}

		// We have to decrement the queue size AFTER we added a possible retry message
		// Otherwise the queueSize could drop to 0 and the system would continue with shutdown
		storageController.incrementDocumentPersisted();
	}

	private void processReceivedTimeout()
	{
		documentMetadataMap.clear();
		documentMetadataMap = null;

		context().parent().tell(ControlMessage.create(ControlMessageType.REMOVE_IDLE_MESSAGE_PROCESSOR, userID), self());
	}
}
