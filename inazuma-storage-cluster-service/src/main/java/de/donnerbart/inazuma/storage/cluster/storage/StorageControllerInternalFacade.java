package de.donnerbart.inazuma.storage.cluster.storage;

import de.donnerbart.inazuma.storage.cluster.storage.wrapper.CouchbaseWrapper;

public interface StorageControllerInternalFacade
{
	public void shutdownCountdown();

	CouchbaseWrapper getCouchbaseWrapper();

	void incrementQueueSize();

	void incrementMetadataRetries();

	void incrementMetadataPersisted();

	void incrementDocumentRetries();

	void incrementDocumentPersisted();

	void incrementDocumentFetched();

	void incrementDataDeleted();
}
