package de.donnerbart.inazuma.storage.base.request;

public interface StorageControllerFacade
{
	public String getDocumentMetadata(String userID);

	public void addDocumentAsync(String userID, String key, String json, long created);

	public String getDocument(String userID, String key);

	public void deleteDocumentAsync(String userID, String key);

	public void markDocumentAsReadAsync(String userID, String key);

	public void shutdown();

	public void awaitShutdown();
}
