package de.donnerbart.inazuma.storage.cluster;

import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static util.AssertionThread.assertEventually;

public class InazumaStorageClusterServiceTest extends BaseIntegrationTest
{
	private final static String ANY_USER_1 = "1000000";
	private final static String ANY_USER_2 = "2000000";

	private final static String DOCUMENT_1_KEY = "11111111-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_1_JSON = "{content:\"mail1\"}";
	private final static long DOCUMENT_1_CREATED = 123456789;

	private final static String DOCUMENT_2_KEY = "22222222-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_2_JSON = "{content:\"mail2\"}";
	private final static long DOCUMENT_2_CREATED = 234567891;

	private final static String DOCUMENT_3_KEY = "33333333-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_3_JSON = "{content:\"mail3\"}";
	private final static long DOCUMENT_3_CREATED = 345678912;

	private final static String DOCUMENT_METADATA_JSON_1;
	private final static String DOCUMENT_METADATA_JSON_1_AND_2;
	private final static String DOCUMENT_METADATA_JSON_3;

	static
	{
		final DocumentMetadata documentMetadata1 = new DocumentMetadata(DOCUMENT_1_CREATED, false);
		final DocumentMetadata documentMetadata2 = new DocumentMetadata(DOCUMENT_2_CREATED, false);
		final DocumentMetadata documentMetadata3 = new DocumentMetadata(DOCUMENT_3_CREATED, false);

		final Map<String, DocumentMetadata> documentMetadataMap1 = new HashMap<>();
		documentMetadataMap1.put(DOCUMENT_1_KEY, documentMetadata1);
		DOCUMENT_METADATA_JSON_1 = GsonWrapper.toJson(documentMetadataMap1);

		final Map<String, DocumentMetadata> documentMetadataMap1And2 = new HashMap<>();
		documentMetadataMap1And2.put(DOCUMENT_1_KEY, documentMetadata1);
		documentMetadataMap1And2.put(DOCUMENT_2_KEY, documentMetadata2);
		DOCUMENT_METADATA_JSON_1_AND_2 = GsonWrapper.toJson(documentMetadataMap1And2);

		final Map<String, DocumentMetadata> documentMetadataMap3 = new HashMap<>();
		documentMetadataMap3.put(DOCUMENT_3_KEY, documentMetadata3);
		DOCUMENT_METADATA_JSON_3 = GsonWrapper.toJson(documentMetadataMap3);
	}

	@Test
	public void addDocument()
	{
		assertDocumentMetadataDoesNotExist(ANY_USER_1);
		assertDocumentDoesNotExist(ANY_USER_1, DOCUMENT_1_KEY);

		requestController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);

		final String document = requestController.getDocument(ANY_USER_1, DOCUMENT_1_KEY);
		assertEquals(document, DOCUMENT_1_JSON);

		final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_1);
		assertEquals(documentMetadata, DOCUMENT_METADATA_JSON_1);
	}

	@Test(dependsOnMethods = {"addDocument"})
	public void addSameDocumentTwiceToSameUser()
	{
		requestController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);

		final String document = requestController.getDocument(ANY_USER_2, DOCUMENT_1_KEY);
		assertEquals(document, DOCUMENT_1_JSON);

		final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_1);
		assertEquals(documentMetadata, DOCUMENT_METADATA_JSON_1);
	}

	@Test(dependsOnMethods = {"addDocument"})
	public void addSecondDocumentToSameUser()
	{
		assertDocumentDoesNotExist(ANY_USER_1, DOCUMENT_2_KEY);

		requestController.addDocument(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);

		final String document = requestController.getDocument(ANY_USER_1, DOCUMENT_2_KEY);
		assertEquals(document, DOCUMENT_2_JSON);

		final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_1);
		assertEquals(documentMetadata, DOCUMENT_METADATA_JSON_1_AND_2);
	}

	@Test
	public void addDocumentEventually()
	{
		assertDocumentMetadataDoesNotExist(ANY_USER_2);
		assertDocumentDoesNotExist(ANY_USER_2, DOCUMENT_3_KEY);

		requestController.addDocument(ANY_USER_2, DOCUMENT_3_KEY, DOCUMENT_3_JSON, DOCUMENT_3_CREATED, AddPersistenceLevel.REQUEST_ON_QUEUE);

		assertEventually(() -> {
			final String document = requestController.getDocument(ANY_USER_2, DOCUMENT_3_KEY);
			assertEquals(document, DOCUMENT_3_JSON);
		});

		assertEventually(() -> {
			final String documentMetadata = requestController.getDocumentMetadata(ANY_USER_2);
			assertEquals(documentMetadata, DOCUMENT_METADATA_JSON_3);
		});
	}
}
