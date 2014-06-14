package request;

import stats.BasicStatisticValue;
import storage.messages.DeleteDocumentMessage;
import storage.messages.PersistDocumentMessage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class RequestController
{
	private final ExecutorService es;

	private final BasicStatisticValue documentAddedRequest = new BasicStatisticValue("RequestController", "documentAddedRequest");
	private final BasicStatisticValue documentFetchedRequest = new BasicStatisticValue("RequestController", "documentFetchedRequest");
	private final BasicStatisticValue documentDeletedRequest = new BasicStatisticValue("RequestController", "documentDeletedRequest");

	public RequestController(ExecutorService es)
	{
		this.es = es;
	}

	public String getDocumentMetadata(final String userID)
	{
		try
		{
			final GetDocumentMetadataTask task = new GetDocumentMetadataTask(userID);
			Future<String> future = es.submit(task);
			return future.get();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public void addDocument(final PersistDocumentMessage message)
	{
		documentAddedRequest.increment();
		final AddDocumentTask task = new AddDocumentTask(message);
		es.submit(task);
	}

	public String getDocument(final String userID, final String key)
	{
		documentFetchedRequest.increment();
		final GetDocumentTask task = new GetDocumentTask(userID, key);
		final Future<String> future = es.submit(task);

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

	public void deleteDocument(final DeleteDocumentMessage message)
	{
		documentDeletedRequest.increment();
		final DeleteDocumentTask task = new DeleteDocumentTask(message);
		es.submit(task);
	}

	public void shutdown()
	{
	}
}
