package request;

public interface StorageControllerFacade
{
	public String getDocumentMetadata(final String userID);

	public void addDocumentAsync(final String userID, final String key, final String json, final long created);

	public String getDocument(final String userID, final String key);

	public void deleteDocumentAsync(final String userID, final String key);
}
