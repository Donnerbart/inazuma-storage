package de.donnerbart.inazuma.storage.cluster.storage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import de.donnerbart.inazuma.storage.base.request.PersistenceLevel;
import de.donnerbart.inazuma.storage.base.request.StorageControllerFacade;
import de.donnerbart.inazuma.storage.base.stats.BasicStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.CustomStatisticValue;
import de.donnerbart.inazuma.storage.cluster.storage.actor.ActorFactory;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ReportWatchCountMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ShutdownMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.user.*;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;

import java.util.concurrent.atomic.LongAdder;

import static de.donnerbart.inazuma.storage.base.util.Watchdog.watchdog;

public class StorageController implements StorageControllerFacade, StorageControllerInternalFacade
{
	private final DatabaseWrapper databaseWrapper;
	private final ActorSystem actorSystem;
	private final ActorRef theReaper;
	private final ActorRef messageDispatcher;

	private final LongAdder queueSize = new LongAdder();

	private final BlockingCallback<Object> callbackAllSoulsReaped = new BlockingCallback<>();
	private final BlockingCallback<Integer> callbackReportWatchedActorCount = new BlockingCallback<>();

	private final BasicStatisticValue documentAdded = BasicStatisticValue.getInstanceOf("StorageController", "documentAdded");
	private final BasicStatisticValue documentFetched = BasicStatisticValue.getInstanceOf("StorageController", "documentFetched");
	private final BasicStatisticValue documentDeleted = BasicStatisticValue.getInstanceOf("StorageController", "documentDeleted");

	private final BasicStatisticValue metadataRetries = BasicStatisticValue.getInstanceOf("StorageController", "retriesMetadata");
	private final BasicStatisticValue metadataPersisted = BasicStatisticValue.getInstanceOf("StorageController", "persistedMetadata");

	private final BasicStatisticValue documentRetries = BasicStatisticValue.getInstanceOf("StorageController", "retriesDocument");
	private final BasicStatisticValue documentPersisted = BasicStatisticValue.getInstanceOf("StorageController", "persistedDocument");

	public StorageController(final DatabaseWrapper databaseWrapper, final int instanceNumber)
	{
		this.databaseWrapper = databaseWrapper;

		this.actorSystem = ActorSystem.create("InazumaStorageCluster" + (instanceNumber > 0 ? instanceNumber : ""));
		actorSystem.eventStream().subscribe(ActorFactory.createDeadLetterListener(actorSystem), DeadLetter.class);

		this.theReaper = ActorFactory.createTheReaper(actorSystem, callbackAllSoulsReaped, callbackReportWatchedActorCount);
		this.messageDispatcher = ActorFactory.createMessageDispatcher(actorSystem, this, theReaper);

		CustomStatisticValue.getInstanceOf("StorageController", "queueSize", new StorageQueueSizeCollector(this));
	}

	@Override
	public String getDocumentMetadata(final String userID)
	{
		final FetchDocumentMetadataMessage message = new FetchDocumentMetadataMessage(userID);
		messageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	public boolean addDocument(final String userID, final String key, final String json, final long created)
	{
		return addDocument(userID, key, json, created, PersistenceLevel.DEFAULT_LEVEL);
	}

	@Override
	public boolean addDocument(final String userID, final String key, final String json, final long created, final PersistenceLevel persistenceLevel)
	{
		queueSize.increment();
		documentAdded.increment();

		final AddDocumentMessage message = new AddDocumentMessage(userID, key, json, created, persistenceLevel);
		messageDispatcher.tell(message, ActorRef.noSender());

		return (persistenceLevel == PersistenceLevel.DOCUMENT_ON_QUEUE) ? true : message.getCallback().getResult();
	}

	@Override
	public String getDocument(final String userID, final String key)
	{
		queueSize.increment();

		final FetchDocumentMessage message = new FetchDocumentMessage(userID, key);
		messageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	@Override
	public void deleteDocumentAsync(final String userID, final String key)
	{
		queueSize.increment();

		final DeleteDocumentMessage message = new DeleteDocumentMessage(userID, key);
		messageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public void markDocumentAsReadAsync(final String userID, final String key)
	{
		queueSize.increment();

		final MarkDocumentAsReadMessage message = new MarkDocumentAsReadMessage(userID, key);
		messageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public void shutdown()
	{
		System.out.println("  Waiting for queue to get empty...");
		watchdog(() -> {
			final long queue = queueSize.sum();
			if (queue > 0)
			{
				System.out.println("  Actual queue size: " + queue);
				return false;
			}
			return true;
		}, 0, 1000);
		System.out.println("  Done!");

		theReaper.tell(ReportWatchCountMessage.getInstance(), ActorRef.noSender());
		messageDispatcher.tell(ShutdownMessage.getInstance(), ActorRef.noSender());

		// Subtract the MessageDispatcher from reported actor count to print the actual number of MessageProcessor actors
		System.out.println("  Waiting for 3 management actors and " + (callbackReportWatchedActorCount.getResult() - 1) + " message processors to finish...");
		callbackAllSoulsReaped.getResult();
		actorSystem.awaitTermination();
		System.out.println("  Done!");
	}

	@Override
	public DatabaseWrapper getDatabaseWrapper()
	{
		return databaseWrapper;
	}

	@Override
	public void incrementQueueSize()
	{
		queueSize.increment();
	}

	@Override
	public void incrementMetadataRetries()
	{
		metadataRetries.increment();
	}

	@Override
	public void incrementMetadataPersisted()
	{
		decrementQueueSize();
		metadataPersisted.increment();
	}

	@Override
	public void incrementDocumentRetries()
	{
		documentRetries.increment();
	}

	@Override
	public void incrementDocumentPersisted()
	{
		decrementQueueSize();
		documentPersisted.increment();
	}

	@Override
	public void incrementDocumentFetched()
	{
		decrementQueueSize();
		documentFetched.increment();
	}

	@Override
	public void incrementDataDeleted()
	{
		decrementQueueSize();
		documentDeleted.increment();
	}

	long getQueueSize()
	{
		return queueSize.sum();
	}

	private void decrementQueueSize()
	{
		queueSize.decrement();
	}
}
