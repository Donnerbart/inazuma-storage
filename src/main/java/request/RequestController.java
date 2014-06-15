package request;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import request.task.AddDocumentTask;
import request.task.DeleteDocumentTask;
import request.task.GetDocumentMetadataTask;
import request.task.GetDocumentTask;
import stats.BasicStatisticValue;

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

	public void addDocument(final String userID, final String key, final String json, final long created)
	{
		documentAddRequest.increment();

		final AddDocumentTask task = new AddDocumentTask(userID, key, json, created);
		es.submitToKeyOwner(task, userID);
	}

	public String getDocument(final String userID, final String key)
	{
		documentGetRequest.increment();

		final GetDocumentTask task = new GetDocumentTask(userID, key);

		return getResultFromCallable(task, key);
	}

	public void deleteDocument(final String userID, final String key)
	{
		documentDeleteRequest.increment();

		final DeleteDocumentTask task = new DeleteDocumentTask(userID, key);
		es.submitToKeyOwner(task, userID);
	}

	public void shutdown()
	{
		INSTANCE.set(null);
		STORAGE_CONTROLLER_INSTANCE.set(null);
	}

	private String getResultFromCallable(final Callable<String> task, final String key)
	{
		final Future<String> future = es.submitToKeyOwner(task, key);

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
