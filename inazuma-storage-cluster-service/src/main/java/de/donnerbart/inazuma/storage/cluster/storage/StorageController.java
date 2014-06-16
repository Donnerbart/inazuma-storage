package de.donnerbart.inazuma.storage.cluster.storage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.couchbase.client.CouchbaseClient;
import de.donnerbart.inazuma.storage.base.request.StorageControllerFacade;
import de.donnerbart.inazuma.storage.base.stats.BasicStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.CustomStatisticValue;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.message.*;
import scala.concurrent.duration.Duration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StorageController implements StorageControllerFacade
{
	private final StorageDBController storageDBController;
	private final ActorSystem actorSystem;
	private final ActorRef storageDispatcher;

	private final AtomicLong queueSize = new AtomicLong(0);
	private final AtomicReference<CountDownLatch> shutdownLatch = new AtomicReference<>(null);

	private final AtomicBoolean notifyEmptyQueue = new AtomicBoolean(false);
	private final Lock shutdownQueueSizeLock = new ReentrantLock();
	private final Condition shutdownQueueSizeCondition = shutdownQueueSizeLock.newCondition();

	private final BasicStatisticValue documentAdded = new BasicStatisticValue("StorageController", "documentAdded");
	private final BasicStatisticValue documentFetched = new BasicStatisticValue("StorageController", "documentFetched");
	private final BasicStatisticValue documentDeleted = new BasicStatisticValue("StorageController", "documentDeleted");

	private final BasicStatisticValue metadataRetries = new BasicStatisticValue("StorageController", "retriesMetadata");
	private final BasicStatisticValue metadataPersisted = new BasicStatisticValue("StorageController", "persistedMetadata");

	private final BasicStatisticValue documentRetries = new BasicStatisticValue("StorageController", "retriesDocument");
	private final BasicStatisticValue documentPersisted = new BasicStatisticValue("StorageController", "persistedDocument");

	private final BasicStatisticValue storageProcessorCreated = new BasicStatisticValue("StorageController", "processorsCreated");
	private final BasicStatisticValue storageProcessorDestroyed = new BasicStatisticValue("StorageController", "processorsDestroyed");

	public StorageController(final CouchbaseClient cb)
	{
		this.actorSystem = ActorSystem.create("InazumaStorage");
		this.storageDispatcher = StorageActorFactory.createStorageDispatcher(actorSystem, this);
		this.storageDBController = new StorageDBController(cb);

		final CustomStatisticValue queueSize = new CustomStatisticValue<>("StorageController", "queueSize", new StorageQueueSizeCollector(this));
		StatisticManager.getInstance().registerStatisticValue(queueSize);
	}

	@Override
	public String getDocumentMetadata(final String userID)
	{
		final BaseCallbackMessage<String> message = new BaseCallbackMessage<>(MessageType.FETCH_DOCUMENT_METADATA, userID);
		storageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	@Override
	public void addDocumentAsync(final String userID, final String key, final String json, final long created)
	{
		queueSize.incrementAndGet();
		documentAdded.increment();

		final AddDocumentMessage message = new AddDocumentMessage(userID, key, json, created);
		storageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public String getDocument(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseCallbackMessageWithKey<String> message = new BaseCallbackMessageWithKey<>(MessageType.FETCH_DOCUMENT, userID, key);
		storageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	@Override
	public void deleteDocumentAsync(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseMessageWithKey message = new BaseMessageWithKey(MessageType.DELETE_DOCUMENT, userID, key);
		storageDispatcher.tell(message, ActorRef.noSender());
	}

	@Override
	public void markDocumentAsReadAsync(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseMessageWithKey message = new BaseMessageWithKey(MessageType.MARK_DOCUMENT_AS_READ, userID, key);
		storageDispatcher.tell(message, ActorRef.noSender());
	}

	public void shutdown()
	{
		final long queue = queueSize.get();
		if (queue > 0)
		{
			System.out.println("Waiting for queue to get empty (" + queue + ")...");
			notifyEmptyQueue.set(true);

			shutdownQueueSizeLock.lock();
			try
			{
				shutdownQueueSizeCondition.await();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
			finally
			{
				shutdownQueueSizeLock.unlock();
			}
		}

		BaseCallbackMessage<Object> message = new BaseCallbackMessage<>(MessageType.SHUTDOWN, null);
		storageDispatcher.tell(message, ActorRef.noSender());

		message.getCallback().getResult();
	}

	public void setShutdownCountdown(final int numberOfActors)
	{
		System.out.println("Killing " + numberOfActors + " running actor(s)...");
		shutdownLatch.set(new CountDownLatch(numberOfActors));
	}

	public void shutdownCountdown()
	{
		final CountDownLatch latch = shutdownLatch.get();
		if (latch != null)
		{
			latch.countDown();
		}
	}

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

	long getQueueSize()
	{
		return queueSize.get();
	}

	StorageDBController getStorageDBController()
	{
		return storageDBController;
	}

	void incrementQueueSize()
	{
		queueSize.incrementAndGet();
	}

	void incrementMetadataRetries()
	{
		metadataRetries.increment();
	}

	void incrementMetadataPersisted()
	{
		decrementQueueSize();
		metadataPersisted.increment();
	}

	void incrementDocumentRetries()
	{
		documentRetries.increment();
	}

	void incrementDocumentPersisted()
	{
		decrementQueueSize();
		documentPersisted.increment();
	}

	void incrementDocumentFetched()
	{
		decrementQueueSize();
		documentFetched.increment();
	}

	void incrementDataDeleted()
	{
		decrementQueueSize();
		documentDeleted.increment();
	}

	void incrementStorageProcessorCreated()
	{
		storageProcessorCreated.increment();
	}

	void incrementStorageProcessorDestroyed()
	{
		storageProcessorDestroyed.increment();
	}

	private void decrementQueueSize()
	{
		final long queue = queueSize.decrementAndGet();
		if (notifyEmptyQueue.get() && queue == 0)
		{
			shutdownQueueSizeLock.lock();
			try
			{
				shutdownQueueSizeCondition.signal();
			}
			finally
			{
				shutdownQueueSizeLock.unlock();
			}
		}
	}
}
