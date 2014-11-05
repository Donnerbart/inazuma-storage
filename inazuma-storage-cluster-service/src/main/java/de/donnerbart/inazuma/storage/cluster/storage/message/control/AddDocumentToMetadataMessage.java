package de.donnerbart.inazuma.storage.cluster.storage.message.control;

import com.hazelcast.core.AsyncCallableCallback;
import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;

public class AddDocumentToMetadataMessage implements ControlMessage
{
	private final String key;
	private final DocumentMetadata metadata;
	private final AddPersistenceLevel persistenceLevel;
	private final AsyncCallableCallback<Boolean> callback;

	public AddDocumentToMetadataMessage(final String key, final DocumentMetadata metadata, final AddPersistenceLevel persistenceLevel, final AsyncCallableCallback<Boolean> callback)
	{
		this.key = key;
		this.metadata = metadata;
		this.persistenceLevel = persistenceLevel;
		this.callback = callback;
	}

	public String getKey()
	{
		return key;
	}

	public DocumentMetadata getMetadata()
	{
		return metadata;
	}

	public AddPersistenceLevel getPersistenceLevel()
	{
		return persistenceLevel;
	}

	public void setResult(boolean result)
	{
		callback.setResult(result);
	}
}
