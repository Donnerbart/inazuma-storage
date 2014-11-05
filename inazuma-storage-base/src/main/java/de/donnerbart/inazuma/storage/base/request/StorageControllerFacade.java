package de.donnerbart.inazuma.storage.base.request;

import com.hazelcast.core.AsyncCallableCallback;

public interface StorageControllerFacade
{
	public String getDocumentMetadata(String userID);

	public void addDocument(String userID, String key, String json, long created, AddPersistenceLevel persistenceLevel, final AsyncCallableCallback<Boolean> callback);

	public String getDocument(String userID, String key);

	public boolean deleteDocument(String userID, String key, final DeletePersistenceLevel persistenceLevel);

	public void markDocumentAsReadAsync(String userID, String key);

	public void shutdown();
}
