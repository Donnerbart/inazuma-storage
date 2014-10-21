package de.donnerbart.inazuma.storage.cluster;

import de.donnerbart.inazuma.storage.base.request.RequestController;
import de.donnerbart.inazuma.storage.cluster.storage.manager.CouchbaseManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

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

	protected InazumaStorageClusterService inazumaStorageClusterService;
	protected RequestController requestController;

	@BeforeClass
	public void beforeClass()
	{
		if (CREATE_BUCKET)
		{
			System.out.println("Listing Couchbase buckets...");
			CouchbaseManager
					.getBucketSettings(CB_USERNAME, CB_PASSWORD)
					.forEach(bucket -> {
						if (BUCKET_NAME.equalsIgnoreCase(bucket.name()))
						{
							fail("Bucket " + BUCKET_NAME + " already exists!");
						}
					});

			System.out.println("Creating test bucket...");
			CouchbaseManager.createBucket(CB_USERNAME, CB_PASSWORD, BUCKET_NAME, 256, true);
			System.out.println("Done!\n");
		}

		inazumaStorageClusterService = new InazumaStorageClusterService(BUCKET_NAME, true);
		requestController = inazumaStorageClusterService.getRequestController();
	}

	@AfterClass
	public void afterClass()
	{
		if (REMOVE_BUCKET)
		{
			System.out.println("Removing test bucket...");
			CouchbaseManager.removeBucket(CB_USERNAME, CB_PASSWORD, BUCKET_NAME);
			System.out.println("Done!\n");
		}

		inazumaStorageClusterService.shutdown();
		inazumaStorageClusterService.await();
	}

	protected void assertDocumentMetadataDoesNotExist(final String userID)
	{
		String documentMetadata = requestController.getDocumentMetadata(userID);
		assertEquals(documentMetadata, "{}");
	}

	protected void assertDocumentDoesNotExist(final String userID, final String key)
	{
		final String document = requestController.getDocument(userID, key);
		assertEquals(document, null);
	}
}
