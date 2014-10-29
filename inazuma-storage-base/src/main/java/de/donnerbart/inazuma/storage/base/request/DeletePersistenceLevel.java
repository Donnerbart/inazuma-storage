package de.donnerbart.inazuma.storage.base.request;

public enum DeletePersistenceLevel
{
	REQUEST_ON_QUEUE,
	DOCUMENT_DELETED,
	DOCUMENT_METADATA_CHANGED;

	public final static DeletePersistenceLevel DEFAULT_LEVEL = DeletePersistenceLevel.DOCUMENT_METADATA_CHANGED;
}
