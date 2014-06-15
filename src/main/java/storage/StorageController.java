package storage;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.couchbase.client.CouchbaseClient;
import scala.concurrent.duration.Duration;
import stats.BasicStatisticValue;
import stats.CustomStatisticValue;
import stats.StatisticManager;
import storage.message.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StorageController
{
	private final StorageDBController storageDBController;
	private final ActorSystem actorSystem;
	private final ActorRef storageDispatcher;

	private final AtomicLong queueSize = new AtomicLong(0);

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

	public String getDocumentMetadata(final String userID)
	{
		final BaseCallbackMessage<String> message = new BaseCallbackMessage<>(MessageType.FETCH_DOCUMENT_METADATA, userID);
		storageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	public void addDocumentAsync(final String userID, final String key, final String json, final long created)
	{
		queueSize.incrementAndGet();
		documentAdded.increment();

		final AddDocumentMessage message = new AddDocumentMessage(userID, key, json, created);
		storageDispatcher.tell(message, ActorRef.noSender());
	}

	public String getDocument(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseCallbackMessageWithKey<String> message = new BaseCallbackMessageWithKey<>(MessageType.FETCH_DOCUMENT, userID, key);
		storageDispatcher.tell(message, ActorRef.noSender());

		return message.getCallback().getResult();
	}

	public void deleteDocumentAsync(final String userID, final String key)
	{
		queueSize.incrementAndGet();

		final BaseMessageWithKey message = new BaseMessageWithKey(MessageType.DELETE_DOCUMENT, userID, key);
		storageDispatcher.tell(message, ActorRef.noSender());
	}

	public void shutdown()
	{
		try
		{
			actorSystem.shutdown();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public void awaitShutdown()
	{
		actorSystem.awaitTermination(Duration.create(60, TimeUnit.MINUTES));
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
		queueSize.decrementAndGet();
		metadataPersisted.increment();
	}

	void incrementDocumentRetries()
	{
		documentRetries.increment();
	}

	void incrementDocumentPersisted()
	{
		queueSize.decrementAndGet();
		documentPersisted.increment();
	}

	void incrementDocumentFetched()
	{
		queueSize.decrementAndGet();
		documentFetched.increment();
	}

	void incrementDataDeleted()
	{
		queueSize.decrementAndGet();
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
}
