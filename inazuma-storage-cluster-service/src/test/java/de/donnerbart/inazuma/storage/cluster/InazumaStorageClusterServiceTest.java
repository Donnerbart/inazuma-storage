package de.donnerbart.inazuma.storage.cluster;

import de.donnerbart.inazuma.storage.base.request.PersistenceLevel;
import org.testng.annotations.Test;

import static util.AssertionThread.assertEventually;
import static org.testng.Assert.assertEquals;

public class InazumaStorageClusterServiceTest extends BaseIntegrationTest
{
	private static final String ANY_USER_1 = "1000000";
	private static final String ANY_USER_2 = "2000000";

	private static final String ANY_DOCUMENT_KEY_1 = "d9185827471";
	private static final String ANY_DOCUMENT_CONTENT_1 = "{key:\"value\"}";
	private static final long ANY_DOCUMENT_CREATED_1 = 1412929912987L;

	private static final String ANY_DOCUMENT_KEY_2 = "a4692546532";
	private static final String ANY_DOCUMENT_CONTENT_2 = "{key:\"value\"}";
	private static final long ANY_DOCUMENT_CREATED_2 = 1412929912988L;

	@Test
	public void addDocumentAndWaitForPersistence()
	{
		assertDocumentMetadataDoesNotExist(ANY_USER_1);
		assertDocumentDoesNotExist(ANY_USER_1, ANY_DOCUMENT_KEY_1);

		requestController.addDocument(ANY_USER_1, ANY_DOCUMENT_KEY_1, ANY_DOCUMENT_CONTENT_1, ANY_DOCUMENT_CREATED_1, PersistenceLevel.DOCUMENT_METADATA_ADDED);

		final String document = requestController.getDocument(ANY_USER_1, ANY_DOCUMENT_KEY_1);
		assertEquals(document, ANY_DOCUMENT_CONTENT_1);

		final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_1);
		assertEquals(documentMetadata, "{\"" + ANY_DOCUMENT_KEY_1 + "\":{\"c\":" + ANY_DOCUMENT_CREATED_1 + ",\"r\":0}}");
	}

	@Test
	public void addDocumentEventually()
	{
		assertDocumentMetadataDoesNotExist(ANY_USER_2);
		assertDocumentDoesNotExist(ANY_USER_2, ANY_DOCUMENT_KEY_2);

		requestController.addDocument(ANY_USER_2, ANY_DOCUMENT_KEY_2, ANY_DOCUMENT_CONTENT_2, ANY_DOCUMENT_CREATED_2, PersistenceLevel.ARRIVED_AT_NODE);

		assertEventually(() -> {
			final String document = requestController.getDocument(ANY_USER_2, ANY_DOCUMENT_KEY_2);
			assertEquals(document, ANY_DOCUMENT_CONTENT_2);
		});

		assertEventually(() -> {
			final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_2);
			assertEquals(documentMetadata, "{\"" + ANY_DOCUMENT_KEY_2 + "\":{\"c\":" + ANY_DOCUMENT_CREATED_2 + ",\"r\":0}}");
		});
	}
}
