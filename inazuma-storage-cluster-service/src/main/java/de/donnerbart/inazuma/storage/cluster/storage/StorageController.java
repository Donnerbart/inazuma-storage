package de.donnerbart.inazuma.storage.cluster.storage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import com.couchbase.client.java.Bucket;
import de.donnerbart.inazuma.storage.base.request.StorageControllerFacade;
import de.donnerbart.inazuma.storage.base.stats.BasicStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.CustomStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.actor.ActorFactory;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.CouchbaseWrapper;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StorageController implements StorageControllerFacade, StorageControllerInternalFacade
{
	private final CouchbaseWrapper couchbaseWrapper;
	private final ActorSystem actorSystem;
	private final ActorRef theReaper;
	private final ActorRef messageDispatcher;

	private final AtomicLong queueSize = new AtomicLong(0);

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

	public StorageController(final Bucket bucket)
	{
		this.couchbaseWrapper = new CouchbaseWrapper(bucket);

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
		final BaseCallbackMessage<String> message = new BaseCallbackMessage<>(MessageType.FETCH_DOCUMENT_METADATA, userID);
		messageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	@Override
	public void addDocumentAsync(final String userID, final String key, final String json, final long created)
	{
		queueSize.incrementAndGet();
		documentAdded.increment();

		final AddDocumentMessage message = new AddDocumentMessage(userID, key, json, created);
		messageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public String getDocument(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseCallbackMessageWithKey<String> message = new BaseCallbackMessageWithKey<>(MessageType.FETCH_DOCUMENT, userID, key);
		messageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	@Override
	public void deleteDocumentAsync(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseMessageWithKey message = new BaseMessageWithKey(MessageType.DELETE_DOCUMENT, userID, key);
		messageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public void markDocumentAsReadAsync(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseMessageWithKey message = new BaseMessageWithKey(MessageType.MARK_DOCUMENT_AS_READ, userID, key);
		messageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public void shutdown()
	{
		final long queue = queueSize.get();
		if (queue > 0)
		{
			System.out.println("Waiting for queue to get empty (" + queue + ")...");
			notifyEmptyQueue.set(true);
			callbackEmptyQueue.getResult();
		}

		messageDispatcher.tell(ControlMessage.create(ControlMessageType.SHUTDOWN), ActorRef.noSender());
	}

	@Override
	public void awaitShutdown()
	{
		theReaper.tell(ControlMessage.create(ControlMessageType.REPORT_WATCH_COUNT), ActorRef.noSender());
		final int actorCount = callbackReportWatchedActorCount.getResult();

		if (actorCount > 0)
		{
			System.out.println("Waiting for actors to finish (" + actorCount + ")...");
			callbackAllSoulsReaped.getResult();
		}

		actorSystem.awaitTermination();
	}

	@Override
	public CouchbaseWrapper getCouchbaseWrapper()
	{
		return couchbaseWrapper;
	}

	@Override
	public void incrementQueueSize()
	{
		queueSize.incrementAndGet();
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
		return queueSize.get();
	}

	private void decrementQueueSize()
	{
		final long queue = queueSize.decrementAndGet();
		if (notifyEmptyQueue.get() && queue == 0)
		{
			callbackEmptyQueue.setResult(null);
		}
	}
}
