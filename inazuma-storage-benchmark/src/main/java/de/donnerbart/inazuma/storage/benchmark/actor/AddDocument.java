package de.donnerbart.inazuma.storage.benchmark.actor;

import akka.actor.UntypedActor;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import de.donnerbart.inazuma.storage.base.request.RequestController;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class AddDocument extends UntypedActor
{
	private static final int MAX_USER = 100000;
	private static final IntObjectOpenHashMap<String> MAILS = new IntObjectOpenHashMap<>();
	private static final Random RANDOM = new Random();

	static
	{
		for (int userID = 1; userID <= MAX_USER; userID++)
		{
			MAILS.put(userID, "{\"userID\":" + userID + "}");
		}
	}

	private final AddDocumentParameters parameters;

	AddDocument(final AddDocumentParameters parameters)
	{
		this.parameters = parameters;
	}

	@Override
	public void onReceive(final Object message) throws Exception
	{
		parameters.getStartLatch().await();

		final int userID = RANDOM.nextInt(MAX_USER) + 1;
		final String key = UUID.randomUUID().toString();
		final long created = (System.currentTimeMillis() / 1000) - RANDOM.nextInt(86400);

		final long started = System.nanoTime();
		RequestController.getInstance().addDocument(String.valueOf(userID), key, MAILS.get(userID), created, parameters.getPersistenceLevel());

		parameters.getDurationAdder().add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
		parameters.getInvocationAdder().increment();

		parameters.getResultLatch().countDown();
	}
}
