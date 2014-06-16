package de.donnerbart.inazuma.storage.base.jmx;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.base.util.NamedThreadFactory;

import java.util.Random;
import java.util.UUID;

public class InazumaStorageRequestWrapper implements InazumaStorageRequestWrapperMBean
{
	private static final int MAX_USER = 100000;
	private static final IntObjectOpenHashMap<String> MAILS = new IntObjectOpenHashMap<>();
	private static final Random generator = new Random();

	static
	{
		for (int userID = 1; userID <= MAX_USER; userID++)
		{
			MAILS.put(userID, "{\"userID\":" + userID + "}");
		}
	}

	@Override
	public String insertSingleDocumentForUser(final int userID)
	{
		final String key = UUID.randomUUID().toString();
		final long created = (System.currentTimeMillis() / 1000) - generator.nextInt(86400);
		RequestController.getInstance().addDocument(String.valueOf(userID), key, MAILS.get(userID), created);

		return key;
	}

	@Override
	public String insertSingleDocument()
	{
		return insertSingleDocumentForUser(createRandomUserID());
	}

	@Override
	public void insertThousandDocuments()
	{
		insertMultipleDocuments(1000);
	}

	@Override
	public void insertMultipleDocuments(final int count)
	{
		final NamedThreadFactory namedThreadFactory = new NamedThreadFactory("DocumentCreator");

		final Thread thread = namedThreadFactory.newThread(new Runnable()
		{
			@Override
			public void run()
			{
				int i = count;
				while (i-- > 0)
				{
					insertSingleDocument();
				}
			}
		});
		thread.start();
	}

	@Override
	public String returnRandomDocumentMetadata()
	{
		return returnDocumentMetadata(String.valueOf(createRandomUserID()));
	}

	@Override
	public String returnDocumentMetadata(final String userID)
	{
		return RequestController.getInstance().getDocumentMetadata(userID);
	}

	@Override
	public String returnDocument(final String userID, final String key)
	{
		return RequestController.getInstance().getDocument(userID, key);
	}

	@Override
	public void deleteDocument(final String userID, final String key)
	{
		RequestController.getInstance().deleteDocument(userID, key);
	}

	private int createRandomUserID()
	{
		return generator.nextInt(MAX_USER) + 1;
	}
}
