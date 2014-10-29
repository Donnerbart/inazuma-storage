package de.donnerbart.inazuma.storage.cluster.storage.message.control;

import de.donnerbart.inazuma.storage.base.request.DeletePersistenceLevel;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;

public class RemoveDocumentFromMetadataMessage implements ControlMessage
{
	private final String key;
	private final DeletePersistenceLevel persistenceLevel;
	private final BlockingCallback<Boolean> callback;

	public RemoveDocumentFromMetadataMessage(final String key, final DeletePersistenceLevel persistenceLevel, final BlockingCallback<Boolean> callback)
	{
		this.key = key;
		this.persistenceLevel = persistenceLevel;
		this.callback = callback;
	}

	public String getKey()
	{
		return key;
	}

	public DeletePersistenceLevel getPersistenceLevel()
	{
		return persistenceLevel;
	}

	public void setResult(boolean result)
	{
		callback.setResult(result);
	}
}
