package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.cluster.storage.StorageControllerInternalFacade;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadataUtil;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.response.DatabaseGetResponse;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
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
		getContext().setReceiveTimeout(Duration.create(5, TimeUnit.MINUTES));
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
					processFetchDocument((BaseMessageWithKey) baseMessage);
					break;
				}
				case PERSIST_DOCUMENT:
				{
					processPersistDocument((AddDocumentMessage) baseMessage);
					break;
				}
				case DELETE_DOCUMENT:
				{
					processDeleteDocument((BaseMessageWithKey) baseMessage);
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
					unhandled(baseMessage);
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
				case REMOVE_DOCUMENT_FROM_METADATA:
					processRemoveDocumentFromMetadata(controlMessage);
					break;
				default:
				{
					unhandled(controlMessage);
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
		getContext().system().scheduler().scheduleOnce(
				Duration.create(DELAY, DELAY_UNIT),
				getSelf(),
				message,
				getContext().system().dispatcher(),
				getSelf()
		);
	}

	private void sendPersistDocumentMetadataMessage()
	{
		if (!persistDocumentMetadataMessageInQueue)
		{
			persistDocumentMetadataMessageInQueue = true;

			storageController.incrementQueueSize();

			getSelf().tell(new BaseMessage(MessageType.PERSIST_DOCUMENT_METADATA, userID), getSelf());
		}
	}

	@SuppressWarnings("unchecked")
	private void processFetchDocument(final BaseMessageWithKey message)
	{
		storageController.getDatabaseWrapper().getDocument(
				message.getKey()
		).subscribe(response -> {
			final String content = ((DatabaseGetResponse) response).getContent();
			((BaseCallbackMessageWithKey<String>) message).setResult(content);

			storageController.incrementDocumentFetched();
		}, e -> {
			log.debug("Could not load document for user {}: {}", userID, e);
			sendDelayedMessage(message);
		});
	}

	private void processPersistDocument(final AddDocumentMessage message)
	{
		storageController.getDatabaseWrapper().insertDocument(
				message.getKey(),
				message.getJson()
		).subscribe(response -> {
			final DocumentMetadata documentMetadata = new DocumentMetadata(message);
			getSelf().tell(new AddDocumentToMetadataControlMessage(message.getKey(), documentMetadata), getSelf());
		}, e -> {
			log.debug("Could not persist document for user {}: {}", userID, e);
			sendDelayedMessage(message);
			storageController.incrementDocumentRetries();
		});
	}

	private void processDeleteDocument(final BaseMessageWithKey message)
	{
		storageController.getDatabaseWrapper().deleteDocument(
				message.getKey()
		).subscribe(response -> {
			getSelf().tell(ControlMessage.create(ControlMessageType.REMOVE_DOCUMENT_FROM_METADATA, message.getKey()), getSelf());
		}, e -> {
			log.debug("Could not delete document {} for user {}: {}", message.getKey(), userID, e);
			sendDelayedMessage(message);
			storageController.incrementDocumentRetries();
		});
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
		storageController.getDatabaseWrapper().insertDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID),
				GsonWrapper.toJson(documentMetadataMap)
		).subscribe(response -> {
			persistDocumentMetadataMessageInQueue = false;

			storageController.incrementMetadataPersisted();
		}, e -> {
			log.debug("Could not store document metadata for user {}: {}", userID, e);
			sendDelayedMessage(message);
			storageController.incrementMetadataRetries();
		});
	}

	private void processLoadDocumentMetadataMessage(final ControlMessage message)
	{
		storageController.getDatabaseWrapper().getDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID)
		).subscribe(response -> {
			getSelf().tell(ControlMessage.create(ControlMessageType.CREATE_METADATA_DOCUMENT, ((DatabaseGetResponse) response).getContent()), getSelf());
		}, e -> {
			log.debug("Could not load document metadata for user {}: {}", userID, e);
			sendDelayedMessage(message);
		});
	}

	private void processCreateDocumentMetadataMessage(final ControlMessage message)
	{
		final String json = message.getContent();
		if (json == null)
		{
			documentMetadataMap = new HashMap<>();

			return;
		}

		documentMetadataMap = GsonWrapper.getDocumentMetadataMap(json);
		if (documentMetadataMap == null)
		{
			log.debug("Could not create document metadata for user {}: {}", userID, json);
			sendDelayedMessage(ControlMessage.create(ControlMessageType.LOAD_DOCUMENT_METADATA));
		}
	}

	private void processAddDocumentToMetadata(final AddDocumentToMetadataControlMessage message)
	{
		documentMetadataMap.put(message.getContent(), message.getMetadata());

		sendPersistDocumentMetadataMessage();

		// We have to decrement the queue size AFTER we added a possible retry message
		// Otherwise the queueSize could drop to 0 and the system would continue with shutdown
		storageController.incrementDocumentPersisted();
	}

	private void processRemoveDocumentFromMetadata(final ControlMessage message)
	{
		documentMetadataMap.remove(message.getContent());

		sendPersistDocumentMetadataMessage();

		storageController.incrementDataDeleted();
	}

	private void processReceivedTimeout()
	{
		documentMetadataMap.clear();
		documentMetadataMap = null;

		getContext().parent().tell(ControlMessage.create(ControlMessageType.REMOVE_IDLE_MESSAGE_PROCESSOR, userID), getSelf());
	}
}
