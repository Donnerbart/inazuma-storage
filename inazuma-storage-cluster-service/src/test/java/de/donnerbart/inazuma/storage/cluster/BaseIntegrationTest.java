package de.donnerbart.inazuma.storage.cluster;

import de.donnerbart.inazuma.storage.cluster.storage.manager.CouchbaseManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class BaseIntegrationTest
{
	private static final String CB_USERNAME;
	private static final String CB_PASSWORD;

	private static final String BUCKET_NAME;
	private static final boolean CREATE_BUCKET;
	private static final boolean REMOVE_BUCKET;

	static
	{
		CB_USERNAME = System.getProperty("username", "Administrator");
		CB_PASSWORD = System.getProperty("password", "");

		BUCKET_NAME = System.getProperty("bucket.name", "inazuma-test");
		CREATE_BUCKET = Boolean.valueOf(System.getProperty("bucket.create", "true"));
		REMOVE_BUCKET = Boolean.valueOf(System.getProperty("bucket.remove", "true"));
	}

	@BeforeClass
	public void beforeClass()
	{
		if (CREATE_BUCKET)
		{
			System.out.println("Creating test bucket...");
			CouchbaseManager.createBucket(CB_USERNAME, CB_PASSWORD, BUCKET_NAME, 256, true);
			System.out.println("Done!");
		}

		InazumaStorageClusterService.start(BUCKET_NAME);
	}

	@AfterClass
	public void afterClass()
	{
		if (REMOVE_BUCKET)
		{
			System.out.println("Removing test bucket...");
			CouchbaseManager.removeBucket(CB_USERNAME, CB_PASSWORD, BUCKET_NAME);
			System.out.println("Done!");
		}

		InazumaStorageClusterService.stopBlocking();
	}
}
