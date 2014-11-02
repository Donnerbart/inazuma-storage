package de.donnerbart.inazuma.storage.benchmark.jmx;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import de.donnerbart.inazuma.storage.base.jmx.InazumaStorageJMXBean;
import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.base.util.NamedThreadFactory;

import java.util.Random;
import java.util.UUID;

public class InazumaStorageBenchmarkWrapper implements InazumaStorageBenchmarkWrapperMBean, InazumaStorageJMXBean
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
		RequestController.getInstance().addDocument(String.valueOf(userID), key, MAILS.get(userID), created, AddPersistenceLevel.REQUEST_ON_QUEUE);

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

		namedThreadFactory.newThread(() -> {
			int i = count;
			while (i-- > 0)
			{
				insertSingleDocument();
			}
		}).start();
	}

	private int createRandomUserID()
	{
		return generator.nextInt(MAX_USER) + 1;
	}
}
