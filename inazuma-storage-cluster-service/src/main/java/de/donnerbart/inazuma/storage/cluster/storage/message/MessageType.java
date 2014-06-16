package de.donnerbart.inazuma.storage.cluster.storage.message;

public enum MessageType
{
	ADD_DOCUMENT,
	DELETE_DOCUMENT,
	FETCH_DOCUMENT,
	FETCH_DOCUMENT_METADATA,
	MARK_DOCUMENT_AS_READ,
	LOAD_DOCUMENT_METADATA,
	PERSIST_DOCUMENT_METADATA,
	STORAGE_PROCESSOR_IDLE
}
