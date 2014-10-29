package de.donnerbart.inazuma.storage.cluster.storage.message.user;

import de.donnerbart.inazuma.storage.base.request.DeletePersistenceLevel;

public class DeleteDocumentMessage extends UserCallbackMessage<Boolean>
{
	private final String key;
	private final DeletePersistenceLevel persistenceLevel;

	public DeleteDocumentMessage(final String userID, final String key, final DeletePersistenceLevel persistenceLevel)
	{
		super(userID);

		this.key = key;
		this.persistenceLevel = persistenceLevel;
	}

	public String getKey()
	{
		return key;
	}

	public DeletePersistenceLevel getPersistenceLevel()
	{
		return persistenceLevel;
	}
}
