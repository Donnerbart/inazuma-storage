package de.donnerbart.inazuma.storage.benchmark.jmx;

@SuppressWarnings("unused")
public interface InazumaStorageBenchmarkWrapperMBean
{
	public String getStatistics();

	public void resetStatistics();

	public int getNumberOfActors();

	public void setNumberOfActors(int threadPoolSize);

	public void insertDocuments1();

	public void insertDocuments1k();

	public void insertDocuments10k();

	public void insertDocuments50k();

	public void insertDocuments100k();

	public void insertMultipleDocuments(int count);
}
