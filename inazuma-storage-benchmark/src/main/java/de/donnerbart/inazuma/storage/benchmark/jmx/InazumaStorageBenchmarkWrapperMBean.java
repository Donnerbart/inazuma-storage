package de.donnerbart.inazuma.storage.benchmark.jmx;

@SuppressWarnings("unused")
public interface InazumaStorageBenchmarkWrapperMBean
{
	public String insertSingleDocumentForUser(int userID);

	public String insertSingleDocument();

	public void insertThousandDocuments();

	public void insertMultipleDocuments(int count);
}
