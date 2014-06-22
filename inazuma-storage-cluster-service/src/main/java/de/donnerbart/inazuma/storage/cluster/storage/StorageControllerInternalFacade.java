package de.donnerbart.inazuma.storage.cluster.storage;

import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;

public interface StorageControllerInternalFacade
{
	DatabaseWrapper getDatabaseWrapper();

	void incrementQueueSize();

	void incrementMetadataRetries();

	void incrementMetadataPersisted();

	void incrementDocumentRetries();

	void incrementDocumentPersisted();

	void incrementDocumentFetched();

	void incrementDataDeleted();
}
