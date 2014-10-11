package de.donnerbart.inazuma.storage.base.request;

public interface StorageControllerFacade
{
	public String getDocumentMetadata(String userID);

	public boolean addDocument(String userID, String key, String json, long created, final PersistenceLevel persistenceLevel);

	public String getDocument(String userID, String key);

	public void deleteDocumentAsync(String userID, String key);

	public void markDocumentAsReadAsync(String userID, String key);

	public void shutdown();
}
