package de.donnerbart.inazuma.storage.cluster.storage;

import com.couchbase.client.CouchbaseClient;
import net.spy.memcached.internal.OperationFuture;

import java.util.concurrent.ExecutionException;

class StorageDBController
{
	private final CouchbaseClient cb;

	public StorageDBController(final CouchbaseClient cb)
	{
		this.cb = cb;
	}

	public String getUserDocumentMetadata(final String userID)
	{
		return (String) cb.get(createDocumentMetadataKey(userID));
	}

	public void storeDocumentMetadata(final String userID, final String document) throws ExecutionException, InterruptedException
	{
		handleOperationFuture(cb.set(createDocumentMetadataKey(userID), 0, document));
	}

	public void storeDocument(final String key, final String document) throws ExecutionException, InterruptedException
	{
		handleOperationFuture(cb.set(key, 0, document));
	}

	public String getDocument(final String key)
	{
		return (String) cb.get(key);
	}

	public void deleteDocument(final String key) throws ExecutionException, InterruptedException
	{
		handleOperationFuture(cb.delete(key));
	}

	private void handleOperationFuture(final OperationFuture<Boolean> future) throws ExecutionException, InterruptedException
	{
		if (!future.get())
		{
			throw new RuntimeException("Got negative result from database operation!");
		}
	}

	private String createDocumentMetadataKey(final String userID)
	{
		return "u-" + userID;
	}
}
