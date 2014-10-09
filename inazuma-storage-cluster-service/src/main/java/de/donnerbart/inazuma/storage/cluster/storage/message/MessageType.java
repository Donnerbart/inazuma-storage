package de.donnerbart.inazuma.storage.cluster.storage.message;

public enum MessageType
{
	FETCH_DOCUMENT,
	PERSIST_DOCUMENT,
	DELETE_DOCUMENT,
	MARK_DOCUMENT_AS_READ,

	FETCH_DOCUMENT_METADATA
}
