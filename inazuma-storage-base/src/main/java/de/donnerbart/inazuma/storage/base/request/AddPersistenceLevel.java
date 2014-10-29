package de.donnerbart.inazuma.storage.base.request;

public enum AddPersistenceLevel
{
	REQUEST_ON_QUEUE,
	DOCUMENT_PERSISTED,
	DOCUMENT_METADATA_ADDED;

	public final static AddPersistenceLevel DEFAULT_LEVEL = AddPersistenceLevel.DOCUMENT_METADATA_ADDED;
}
