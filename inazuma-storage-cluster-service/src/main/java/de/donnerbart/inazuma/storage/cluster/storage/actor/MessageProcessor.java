package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.donnerbart.inazuma.storage.base.request.DeletePersistenceLevel;
import de.donnerbart.inazuma.storage.base.request.PersistenceLevel;
import de.donnerbart.inazuma.storage.cluster.storage.StorageControllerInternalFacade;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.*;
import de.donnerbart.inazuma.storage.cluster.storage.message.user.*;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadataUtil;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
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

/*
	@Override
	public void postStop() throws Exception
	{
		super.postStop();
		System.out.println("    The MessageProcessor for " + userID + " is has stopped!");
	}
*/

	@Override
	public void onReceive(final Object message) throws Exception
	{
		//System.err.println(userID + ": " + message);

		if (message instanceof UserMessage)
		{
			if (documentMetadataMap == null)
			{
				sendDelayedMessage(message);
				return;
			}

			if (message instanceof FetchDocumentMessage)
			{
				processFetchDocument((FetchDocumentMessage) message);
			}
			else if (message instanceof AddDocumentMessage)
			{
				processPersistDocument((AddDocumentMessage) message);
			}
			else if (message instanceof DeleteDocumentMessage)
			{
				processDeleteDocument((DeleteDocumentMessage) message);
			}
			else if (message instanceof MarkDocumentAsReadMessage)
			{
				processMarkDocumentAsRead((MarkDocumentAsReadMessage) message);
			}
			else if (message instanceof FetchDocumentMetadataMessage)
			{
				processFetchDocumentMetadata((FetchDocumentMetadataMessage) message);
			}
			else
			{
				unhandled(message);
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
		else if (message instanceof PersistDocumentMetadataMessage)
		{
			processPersistDocumentMetadata(message);
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

			getSelf().tell(PersistDocumentMetadataMessage.getInstance(), getSelf());
		}
	}

	private void processFetchDocument(final FetchDocumentMessage message)
	{
		databaseWrapper.getDocument(
				message.getKey()
		).subscribe(response -> {
			message.setResult(response);

			storageController.incrementDocumentFetched();
		}, e -> {
			log.warning("Could not load document for user {}: {}", userID, e);
			sendDelayedMessage(message);
		});
	}

	private void processPersistDocument(final AddDocumentMessage message)
	{
		databaseWrapper.insertDocument(
				message.getKey(),
				message.getJson()
		).subscribe(response -> {
			if (message.getPersistenceLevel() == PersistenceLevel.DOCUMENT_PERSISTED)
			{
				message.setResult(true);
			}

			final ControlMessage controlMessage = new AddDocumentToMetadataMessage(
					message.getKey(),
					new DocumentMetadata(message),
					message.getPersistenceLevel(),
					message.getCallback()
			);
			getSelf().tell(controlMessage, getSelf());
		}, e -> {
			log.warning("Could not persist document for user {}: {}", userID, e);
			sendDelayedMessage(message);
			storageController.incrementDocumentRetries();
		});
	}

	private void processDeleteDocument(final DeleteDocumentMessage message)
	{
		databaseWrapper.deleteDocument(
				message.getKey()
		).subscribe(response -> {
			if (message.getDeletePersistenceLevel() == DeletePersistenceLevel.DOCUMENT_DELETED)
			{
				message.setResult(true);
			}

			final ControlMessage controlMessage = new RemoveDocumentFromMetadataMessage(
					message.getKey(),
					message.getDeletePersistenceLevel(),
					message.getCallback()
			);
			getSelf().tell(controlMessage, getSelf());
		}, e -> {
			log.warning("Could not delete document {} for user {}: {}", message.getKey(), userID, e);
			sendDelayedMessage(message);
			storageController.incrementDocumentRetries();
		});
	}

	private void processMarkDocumentAsRead(final MarkDocumentAsReadMessage baseMessage)
	{
		documentMetadataMap.get(baseMessage.getKey()).setRead(true);

		sendPersistDocumentMetadataMessage();
	}

	private void processFetchDocumentMetadata(final FetchDocumentMetadataMessage message)
	{
		message.setResult(GsonWrapper.toJson(documentMetadataMap));
	}

	private void processPersistDocumentMetadata(final Object message)
	{
		if (documentMetadataMap == null)
		{
			log.warning("Could not persist empty document metadata for user {}", userID);

			sendDelayedMessage(message);
			return;
		}

		databaseWrapper.insertDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID),
				GsonWrapper.toJson(documentMetadataMap)
		).subscribe(response -> {
			persistDocumentMetadataMessageInQueue = false;

			storageController.incrementMetadataPersisted();
		}, e -> {
			log.warning("Could not store document metadata for user {}: {}", userID, e);
			sendDelayedMessage(message);
			storageController.incrementMetadataRetries();
		});
	}

	private void processLoadDocumentMetadataMessage(final Object message)
	{
		databaseWrapper.getDocument(
				DocumentMetadataUtil.createKeyFromUserID(userID)
		).subscribe(response -> {
			final ControlMessage controlMessage = new CreateMetadataDocumentMessage(response);
			getSelf().tell(controlMessage, getSelf());
		}, e -> {
			log.warning("Could not load document metadata for user {}: {}", userID, e);
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
			log.warning("Could not create document metadata for user {}: {}", userID, json);
			sendDelayedMessage(LoadDocumentMetadataMessage.getInstance());
		}
	}

	private void processAddDocumentToMetadata(final AddDocumentToMetadataMessage message)
	{
		documentMetadataMap.put(message.getKey(), message.getMetadata());

		if (message.getPersistenceLevel() == PersistenceLevel.DOCUMENT_METADATA_ADDED)
		{
			message.setResult(true);
		}

		sendPersistDocumentMetadataMessage();

		// We have to decrement the queue size AFTER we added a possible retry message
		// Otherwise the queueSize could drop to 0 and the system would continue with shutdown
		storageController.incrementDocumentPersisted();
	}

	private void processRemoveDocumentFromMetadata(final RemoveDocumentFromMetadataMessage message)
	{
		documentMetadataMap.remove(message.getKey());

		if (message.getDeletePersistenceLevel() == DeletePersistenceLevel.DOCUMENT_METADATA_CHANGED)
		{
			message.setResult(true);
		}

		sendPersistDocumentMetadataMessage();

		storageController.incrementDataDeleted();
	}

	private void processReceivedTimeout()
	{
		if (documentMetadataMap != null)
		{
			documentMetadataMap.clear();
			documentMetadataMap = null;
		}

		getContext().parent().tell(new RemoveIdleMessageProcessorMessage(userID), getSelf());
	}
}
