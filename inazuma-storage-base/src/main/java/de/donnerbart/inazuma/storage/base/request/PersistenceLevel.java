package de.donnerbart.inazuma.storage.base.request;

public enum PersistenceLevel
{
	DOCUMENT_ON_QUEUE,
	DOCUMENT_PERSISTED,
	DOCUMENT_METADATA_ADDED;

	public final static PersistenceLevel DEFAULT_LEVEL = PersistenceLevel.DOCUMENT_METADATA_ADDED;
}
