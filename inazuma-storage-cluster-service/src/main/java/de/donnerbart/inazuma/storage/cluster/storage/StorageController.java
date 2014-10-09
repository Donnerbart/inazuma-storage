package de.donnerbart.inazuma.storage.cluster.storage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import de.donnerbart.inazuma.storage.base.request.StorageControllerFacade;
import de.donnerbart.inazuma.storage.base.stats.BasicStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.CustomStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.actor.ActorFactory;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ReportWatchCountMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.control.ShutdownMessage;
import de.donnerbart.inazuma.storage.cluster.storage.message.user.*;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class StorageController implements StorageControllerFacade, StorageControllerInternalFacade
{
	private final DatabaseWrapper databaseWrapper;
	private final ActorSystem actorSystem;
	private final ActorRef theReaper;
	private final ActorRef messageDispatcher;

	private final LongAdder queueSize = new LongAdder();

	private final AtomicBoolean notifyEmptyQueue = new AtomicBoolean(false);
	private final BlockingCallback<Object> callbackEmptyQueue = new BlockingCallback<>();
	private final BlockingCallback<Object> callbackAllSoulsReaped = new BlockingCallback<>();
	private final BlockingCallback<Integer> callbackReportWatchedActorCount = new BlockingCallback<>();

	private final BasicStatisticValue documentAdded = new BasicStatisticValue("StorageController", "documentAdded");
	private final BasicStatisticValue documentFetched = new BasicStatisticValue("StorageController", "documentFetched");
	private final BasicStatisticValue documentDeleted = new BasicStatisticValue("StorageController", "documentDeleted");

	private final BasicStatisticValue metadataRetries = new BasicStatisticValue("StorageController", "retriesMetadata");
	private final BasicStatisticValue metadataPersisted = new BasicStatisticValue("StorageController", "persistedMetadata");

	private final BasicStatisticValue documentRetries = new BasicStatisticValue("StorageController", "retriesDocument");
	private final BasicStatisticValue documentPersisted = new BasicStatisticValue("StorageController", "persistedDocument");

	public StorageController(final DatabaseWrapper databaseWrapper)
	{
		this.databaseWrapper = databaseWrapper;

		this.actorSystem = ActorSystem.create("InazumaStorageCluster");
		actorSystem.eventStream().subscribe(ActorFactory.createDeadLetterListener(actorSystem), DeadLetter.class);

		this.theReaper = ActorFactory.createTheReaper(actorSystem, callbackAllSoulsReaped, callbackReportWatchedActorCount);
		this.messageDispatcher = ActorFactory.createMessageDispatcher(actorSystem, this, theReaper);

		final CustomStatisticValue queueSize = new CustomStatisticValue<>("StorageController", "queueSize", new StorageQueueSizeCollector(this));
		StatisticManager.getInstance().registerStatisticValue(queueSize);
	}

	@Override
	public String getDocumentMetadata(final String userID)
	{
		final FetchDocumentMetadataMessage message = new FetchDocumentMetadataMessage(userID);
		messageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	@Override
	public void addDocumentAsync(final String userID, final String key, final String json, final long created)
	{
		queueSize.increment();
		documentAdded.increment();

		final AddDocumentMessage message = new AddDocumentMessage(userID, key, json, created);
		messageDispatcher.tell(message, ActorRef.noSender());
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
		final long queue = queueSize.sum();
		if (queue > 0)
		{
			System.out.println("Waiting for queue to get empty (" + queue + ")...");
			notifyEmptyQueue.set(true);
			callbackEmptyQueue.getResult();
		}

		messageDispatcher.tell(ShutdownMessage.getInstance(), ActorRef.noSender());
	}

	@Override
	public void awaitShutdown()
	{
		theReaper.tell(ReportWatchCountMessage.getInstance(), ActorRef.noSender());
		final int actorCount = callbackReportWatchedActorCount.getResult();

		if (actorCount > 0)
		{
			System.out.println("Waiting for actors to finish (" + actorCount + ")...");
			callbackAllSoulsReaped.getResult();
		}

		actorSystem.awaitTermination();
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
		final long queue = queueSize.sum();
		if (notifyEmptyQueue.get() && queue == 0)
		{
			callbackEmptyQueue.setResult(null);
		}
	}
}
