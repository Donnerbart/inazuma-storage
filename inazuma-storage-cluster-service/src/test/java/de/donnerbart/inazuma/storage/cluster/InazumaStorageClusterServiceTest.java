package de.donnerbart.inazuma.storage.cluster;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class InazumaStorageClusterServiceTest extends BaseIntegrationTest
{
	private static final String ANY_USER_1 = "1000000";
	private static final String ANY_DOCUMENT_KEY_1 = "d9185827471";
	private static final String ANY_DOCUMENT_CONTENT_1 = "{key:\"value\"}";
	private static final long ANY_DOCUMENT_CREATED_1 = 1412929912987L;

	@Test
	public void addDocument()
	{
		assertDocumentMetadataDoesNotExist(ANY_USER_1);
		assertDocumentDoesNotExist(ANY_USER_1, ANY_DOCUMENT_KEY_1);

		requestController.addDocument(ANY_USER_1, ANY_DOCUMENT_KEY_1, ANY_DOCUMENT_CONTENT_1, ANY_DOCUMENT_CREATED_1);
		try
		{
			Thread.sleep(500);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_1);
		assertEquals(documentMetadata, "{\"" + ANY_DOCUMENT_KEY_1 + "\":{\"c\":" + ANY_DOCUMENT_CREATED_1 + ",\"r\":0}}");

		final String document = requestController.getDocument(ANY_USER_1, ANY_DOCUMENT_KEY_1);
		assertEquals(document, ANY_DOCUMENT_CONTENT_1);
	}
}
