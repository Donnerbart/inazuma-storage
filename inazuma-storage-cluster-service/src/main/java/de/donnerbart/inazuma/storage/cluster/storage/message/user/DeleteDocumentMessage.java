package de.donnerbart.inazuma.storage.cluster.storage.message.user;

import de.donnerbart.inazuma.storage.base.request.DeletePersistenceLevel;

public class DeleteDocumentMessage extends UserCallbackMessage<Boolean>
{
	private final String key;
	private final DeletePersistenceLevel deletePersistenceLevel;

	public DeleteDocumentMessage(final String userID, final String key, final DeletePersistenceLevel deletePersistenceLevel)
	{
		super(userID);

		this.key = key;
		this.deletePersistenceLevel = deletePersistenceLevel;
	}

	public String getKey()
	{
		return key;
	}

	public DeletePersistenceLevel getDeletePersistenceLevel()
	{
		return deletePersistenceLevel;
	}
}
