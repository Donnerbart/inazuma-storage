package de.donnerbart.inazuma.storage.base.jmx;

@SuppressWarnings("unused")
public interface InazumaStorageRequestWrapperMBean
{
	public String insertSingleDocumentForUser(int userID);

	public String returnRandomDocumentMetadata();

	public String returnDocumentMetadata(String userID);

	public String returnDocument(String userID, String key);

	void deleteDocument(String userID, String key);

	void markDocumentAsRead(String userID, String key);
}
