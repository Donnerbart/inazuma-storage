package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.cluster.storage.StorageControllerInternalFacade;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.*;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadataUtil;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;
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

	private final DatabaseWrapper databaseWrapper;

	private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	private Map<String, DocumentMetadata> documentMetadataMap = null;
	private boolean persistDocumentMetadataMessageInQueue = false;

	public MessageProcessor(final StorageControllerInternalFacade storageController, final String userID)
	{
		this.storageController = storageController;
		this.userID = userID;

		this.databaseWrapper = storageController.getDatabaseWrapper();

		processLoadDocumentMetadataMessage(LoadDocumentMetadataMessage.getInstance());
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
					processFetchDocument((FetchDocumentMessage) baseMessage);
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
					processFetchDocumentMetadata((FetchDocumentMetadataMessage) baseMessage);
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
		else if (message instanceof LoadDocumentMetadataMessage)
		{
			processLoadDocumentMetadataMessage(message);
		}
		else if (message instanceof CreateMetadataDocumentMessage)
		{
			processCreateDocumentMetadataMessage((CreateMetadataDocumentMessage) message);
		}
		else if (message instanceof AddDocumentToMetadataMessage)
		{
			processAddDocumentToMetadata((AddDocumentToMetadataMessage) message);
		}
		else if (message instanceof RemoveDocumentFromMetadataMessage)
		{
			processRemoveDocumentFromMetadata((RemoveDocumentFromMetadataMessage) message);
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

	private void processFetchDocument(final FetchDocumentMessage message)
	{
		databaseWrapper.getDocument(
				message.getKey()
		).subscribe(response -> {
			final String content = ((DatabaseGetResponse) response).getContent();
			message.setResult(content);

			storageController.incrementDocumentFetched();
		}, e -> {
			log.debug("Could not load document for user {}: {}", userID, e);
			sendDelayedMessage(message);
		});
	}

	private void processPersistDocument(final AddDocumentMessage message)
	{
		databaseWrapper.insertDocument(
				message.getKey(),
				message.getJson()
		).subscribe(response -> {
			final ControlMessage controlMessage = new AddDocumentToMetadataMessage(message.getKey(), new DocumentMetadata(message));
			getSelf().tell(controlMessage, getSelf());
		}, e -> {
			log.debug("Could not persist document for user {}: {}", userID, e);
			sendDelayedMessage(message);
			storageController.incrementDocumentRetries();
		});
	}

	private void processDeleteDocument(final BaseMessageWithKey message)
	{
		databaseWrapper.deleteDocument(
				message.getKey()
		).subscribe(response -> {
			final ControlMessage controlMessage = new RemoveDocumentFromMetadataMessage(message.getKey());
			getSelf().tell(controlMessage, getSelf());
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

	private void processFetchDocumentMetadata(final FetchDocumentMetadataMessage message)
	{
		message.setResult(GsonWrapper.toJson(documentMetadataMap));
	}

	private void processPersistDocumentMetadata(final BaseMessage message)
	{
		databaseWrapper.insertDocument(
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

	private void processLoadDocumentMetadataMessage(final Object message)
	{
		databaseWrapper.getDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID)
		).subscribe(response -> {
			final ControlMessage controlMessage = new CreateMetadataDocumentMessage(((DatabaseGetResponse) response).getContent());
			getSelf().tell(controlMessage, getSelf());
		}, e -> {
			log.debug("Could not load document metadata for user {}: {}", userID, e);
			sendDelayedMessage(message);
		});
	}

	private void processCreateDocumentMetadataMessage(final CreateMetadataDocumentMessage message)
	{
		final String json = message.getJson();
		if (json == null)
		{
			documentMetadataMap = new HashMap<>();

			return;
		}

		documentMetadataMap = GsonWrapper.getDocumentMetadataMap(json);
		if (documentMetadataMap == null)
		{
			log.debug("Could not create document metadata for user {}: {}", userID, json);
			sendDelayedMessage(LoadDocumentMetadataMessage.getInstance());
		}
	}

	private void processAddDocumentToMetadata(final AddDocumentToMetadataMessage message)
	{
		documentMetadataMap.put(message.getId(), message.getMetadata());

		sendPersistDocumentMetadataMessage();

		// We have to decrement the queue size AFTER we added a possible retry message
		// Otherwise the queueSize could drop to 0 and the system would continue with shutdown
		storageController.incrementDocumentPersisted();
	}

	private void processRemoveDocumentFromMetadata(final RemoveDocumentFromMetadataMessage message)
	{
		documentMetadataMap.remove(message.getId());

		sendPersistDocumentMetadataMessage();

		storageController.incrementDataDeleted();
	}

	private void processReceivedTimeout()
	{
		documentMetadataMap.clear();
		documentMetadataMap = null;

		getContext().parent().tell(new RemoveIdleMessageProcessorMessage(userID), getSelf());
	}
}
