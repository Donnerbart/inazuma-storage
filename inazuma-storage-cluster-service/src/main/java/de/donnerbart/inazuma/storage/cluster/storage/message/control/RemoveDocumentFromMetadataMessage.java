package de.donnerbart.inazuma.storage.cluster.storage.message.control;

import de.donnerbart.inazuma.storage.base.request.DeletePersistenceLevel;
import de.donnerbart.inazuma.storage.cluster.storage.callback.BlockingCallback;

public class RemoveDocumentFromMetadataMessage implements ControlMessage
{
	private final String key;
	private final DeletePersistenceLevel deletePersistenceLevel;
	private final BlockingCallback<Boolean> callback;

	public RemoveDocumentFromMetadataMessage(final String key, final DeletePersistenceLevel deletePersistenceLevel, final BlockingCallback<Boolean> callback)
	{
		this.key = key;
		this.deletePersistenceLevel = deletePersistenceLevel;
		this.callback = callback;
	}

	public String getKey()
	{
		return key;
	}

	public DeletePersistenceLevel getDeletePersistenceLevel()
	{
		return deletePersistenceLevel;
	}

	public void setResult(boolean result)
	{
		callback.setResult(result);
	}
}
