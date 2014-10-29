package de.donnerbart.inazuma.storage.base.request;

public interface StorageControllerFacade
{
	public String getDocumentMetadata(String userID);

	public boolean addDocument(String userID, String key, String json, long created, AddPersistenceLevel persistenceLevel);

	public String getDocument(String userID, String key);

	public boolean deleteDocument(String userID, String key, final DeletePersistenceLevel persistenceLevel);

	public void markDocumentAsReadAsync(String userID, String key);

	public void shutdown();
}
