package de.donnerbart.inazuma.storage.base.request;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import de.donnerbart.inazuma.storage.base.request.task.*;
import de.donnerbart.inazuma.storage.base.stats.BasicStatisticValue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class RequestController
{
	private static final AtomicReference<RequestController> INSTANCE = new AtomicReference<>(null);
	private static final AtomicReference<StorageControllerFacade> STORAGE_CONTROLLER_INSTANCE = new AtomicReference<>(null);

	private final IExecutorService es;

	private final BasicStatisticValue documentAddRequest = new BasicStatisticValue("RequestController", "documentAddRequest");
	private final BasicStatisticValue documentGetRequest = new BasicStatisticValue("RequestController", "documentGetRequest");
	private final BasicStatisticValue documentDeleteRequest = new BasicStatisticValue("RequestController", "documentDeleteRequest");

	public static RequestController getInstance()
	{
		return INSTANCE.get();
	}

	public static StorageControllerFacade getStorageControllerInstance()
	{
		return STORAGE_CONTROLLER_INSTANCE.get();
	}

	public RequestController(final HazelcastInstance hz, final StorageControllerFacade storageController)
	{
		this.es = hz.getExecutorService("inazumaExecutor");

		STORAGE_CONTROLLER_INSTANCE.set(storageController);
		INSTANCE.set(this);
	}

	public String getDocumentMetadata(final String userID)
	{
		final GetDocumentMetadataTask task = new GetDocumentMetadataTask(userID);

		return getResultFromCallable(task, userID);
	}

	public boolean addDocument(final String userID, final String key, final String json, final long created)
	{
		return addDocument(userID, key, json, created, PersistenceLevel.DEFAULT_LEVEL);
	}

	public boolean addDocument(final String userID, final String key, final String json, final long created, final PersistenceLevel persistenceLevel)
	{
		documentAddRequest.increment();

		final AddDocumentTask task = new AddDocumentTask(userID, key, json, created, persistenceLevel);

		return getResultFromCallable(task, userID);
	}

	public String getDocument(final String userID, final String key)
	{
		documentGetRequest.increment();

		final GetDocumentTask task = new GetDocumentTask(userID, key);

		return getResultFromCallable(task, userID);
	}

	public void deleteDocument(final String userID, final String key)
	{
		documentDeleteRequest.increment();

		final DeleteDocumentTask task = new DeleteDocumentTask(userID, key);
		es.submitToKeyOwner(task, userID);
	}

	public void markDocumentAsRead(final String userID, final String key)
	{
		final MarkDocumentAsReadTask task = new MarkDocumentAsReadTask(userID, key);
		es.submitToKeyOwner(task, userID);
	}

	public void shutdown()
	{
		INSTANCE.set(null);
		STORAGE_CONTROLLER_INSTANCE.set(null);
	}

	private <T> T getResultFromCallable(final Callable<T> task, final String key)
	{
		final Future<T> future = es.submitToKeyOwner(task, key);

		try
		{
			return future.get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			e.printStackTrace();
		}

		return null;
	}
}
