package de.donnerbart.inazuma.storage.cluster.storage.message;

public enum ControlMessageType
{
	WATCH_ME,
	REPORT_WATCH_COUNT,

	REMOVE_IDLE_MESSAGE_PROCESSOR,
	SHUTDOWN,

	LOAD_DOCUMENT_METADATA,
	CREATE_METADATA_DOCUMENT,
	ADD_DOCUMENT_TO_METADATA
}
