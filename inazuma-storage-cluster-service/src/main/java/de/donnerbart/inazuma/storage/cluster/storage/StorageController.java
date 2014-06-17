package de.donnerbart.inazuma.storage.cluster.storage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import com.couchbase.client.CouchbaseClient;
import de.donnerbart.inazuma.storage.base.request.StorageControllerFacade;
import de.donnerbart.inazuma.storage.base.stats.BasicStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.CustomStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.actor.ActorFactory;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.CouchbaseWrapper;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class StorageController implements StorageControllerFacade, StorageControllerInternalFacade
{
	private final CouchbaseWrapper couchbaseWrapper;
	private final ActorSystem actorSystem;
	private final ActorRef messageDispatcher;

	private final AtomicLong queueSize = new AtomicLong(0);
	private final AtomicReference<CountDownLatch> shutdownLatch = new AtomicReference<>(null);

	private final AtomicBoolean notifyEmptyQueue = new AtomicBoolean(false);
	private final AtomicReference<BlockingCallback<Object>> callbackEmptyQueue = new AtomicReference<>(new BlockingCallback<>());

	private final BasicStatisticValue documentAdded = new BasicStatisticValue("StorageController", "documentAdded");
	private final BasicStatisticValue documentFetched = new BasicStatisticValue("StorageController", "documentFetched");
	private final BasicStatisticValue documentDeleted = new BasicStatisticValue("StorageController", "documentDeleted");

	private final BasicStatisticValue metadataRetries = new BasicStatisticValue("StorageController", "retriesMetadata");
	private final BasicStatisticValue metadataPersisted = new BasicStatisticValue("StorageController", "persistedMetadata");

	private final BasicStatisticValue documentRetries = new BasicStatisticValue("StorageController", "retriesDocument");
	private final BasicStatisticValue documentPersisted = new BasicStatisticValue("StorageController", "persistedDocument");

	public StorageController(final CouchbaseClient cb)
	{
		this.actorSystem = ActorSystem.create("InazumaStorageCluster");
		this.messageDispatcher = ActorFactory.createMessageDispatcher(actorSystem, this);
		this.couchbaseWrapper = new CouchbaseWrapper(cb);

		final CustomStatisticValue queueSize = new CustomStatisticValue<>("StorageController", "queueSize", new StorageQueueSizeCollector(this));
		StatisticManager.getInstance().registerStatisticValue(queueSize);

		actorSystem.eventStream().subscribe(ActorFactory.createDeadLetterListener(actorSystem), DeadLetter.class);
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
			callbackEmptyQueue.get().getResult();
		}

		BaseCallbackMessage<Object> message = new BaseCallbackMessage<>(MessageType.SHUTDOWN, null);
		messageDispatcher.tell(message, ActorRef.noSender());

		message.getCallback().getResult();
	}

	public void setShutdownCountdown(final int numberOfActors)
	{
		System.out.println("Killing " + numberOfActors + " running actor(s)...");
		shutdownLatch.set(new CountDownLatch(numberOfActors));
	}

	@Override
	public void shutdownCountdown()
	{
		final CountDownLatch latch = shutdownLatch.get();
		if (latch != null)
		{
			latch.countDown();
		}
	}

	@Override
	public void awaitShutdown()
	{
		try
		{
			shutdownLatch.get().await();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		actorSystem.shutdown();
		actorSystem.awaitTermination(Duration.create(10, TimeUnit.SECONDS));
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
			callbackEmptyQueue.get().setResult(null);
		}
	}
}
