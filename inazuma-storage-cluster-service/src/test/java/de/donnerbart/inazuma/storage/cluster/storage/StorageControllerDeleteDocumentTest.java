package de.donnerbart.inazuma.storage.cluster.storage;

import com.couchbase.client.core.BackpressureException;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static util.AssertionThread.assertEventually;

public class StorageControllerDeleteDocumentTest
{
	private static final Observable<String> DATABASE_GET_RESPONSE_SUCCESS = Observable.just(null);
	private static final Observable<Boolean> DATABASE_RESPONSE_SUCCESS = Observable.just(Boolean.TRUE);
	private static final Observable<Boolean> DATABASE_RESPONSE_FAILURE = Observable.error(new BackpressureException());

	private final static String ANY_USER = "1000000";
	private final static String DOCUMENT_METADATA_KEY_USER = "u-" + ANY_USER;

	private static final String DOCUMENT_METADATA_JSON_EMPTY = "{}";

	private final static String DOCUMENT_1_KEY = "11111111-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_1_JSON = "{content:\"mail1\"}";
	private final static long DOCUMENT_1_CREATED = 123456789;
	private final static DocumentMetadata DOCUMENT_1_METADATA = new DocumentMetadata(DOCUMENT_1_CREATED, false);

	private final static String DOCUMENT_2_KEY = "22222222-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_2_JSON = "{content:\"mail2\"}";
	private final static long DOCUMENT_2_CREATED = 234567891;
	private final static DocumentMetadata DOCUMENT_2_METADATA = new DocumentMetadata(DOCUMENT_2_CREATED, false);

	private final static String DOCUMENT_METADATA_JSON_1;
	private final static String DOCUMENT_METADATA_JSON_2;
	private final static String DOCUMENT_METADATA_JSON_1_AND_2;

	static
	{
		final Map<String, DocumentMetadata> documentMetadataMap1 = new HashMap<>();
		documentMetadataMap1.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
		DOCUMENT_METADATA_JSON_1 = GsonWrapper.toJson(documentMetadataMap1);

		final Map<String, DocumentMetadata> documentMetadataMap2 = new HashMap<>();
		documentMetadataMap2.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
		DOCUMENT_METADATA_JSON_2 = GsonWrapper.toJson(documentMetadataMap2);

		final Map<String, DocumentMetadata> documentMetadataMap1And2 = new HashMap<>();
		documentMetadataMap1And2.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
		documentMetadataMap1And2.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
		DOCUMENT_METADATA_JSON_1_AND_2 = GsonWrapper.toJson(documentMetadataMap1And2);
	}

	@Mock
	private DatabaseWrapper databaseWrapper;

	private StorageController storageController;

	private DatabaseFailureMultipleTimesAnswer<Boolean> databaseFailOnceAnswer;

	@BeforeMethod
	public void setUp() throws Exception
	{
		MockitoAnnotations.initMocks(this);

		storageController = new StorageController(databaseWrapper, 0);

		databaseFailOnceAnswer = new DatabaseFailureMultipleTimesAnswer<>(DATABASE_RESPONSE_SUCCESS, DATABASE_RESPONSE_FAILURE);
	}

	@AfterMethod
	public void tearDown()
	{
		StatisticManager.getInstance().shutdown();
	}

	@Test
	public void addAndDeleteDocument()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_EMPTY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);

		// Check that user has no document and no metadata
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_EMPTY);

		// Add document and check the result
		storageController.addDocument(ANY_USER, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_1);

		// Delete document and check the result
		storageController.deleteDocumentAsync(ANY_USER, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_EMPTY);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER);
		verify(databaseWrapper, times(3)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_EMPTY);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void deleteAlreadyPersistedDocument()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER)).thenReturn(Observable.just(DOCUMENT_METADATA_JSON_1));
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_EMPTY)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		// Check that user has one document and metadata for this document
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_1);

		// Delete document and check the result
		storageController.deleteDocumentAsync(ANY_USER, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_EMPTY);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER);
		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_EMPTY);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void deleteOneOfTwoAlreadyPersistedDocuments()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER)).thenReturn(Observable.just(DOCUMENT_METADATA_JSON_1_AND_2));
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_2_KEY)).thenReturn(Observable.just(DOCUMENT_2_JSON));
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_2)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		// Check that user has two documents and metadata for those documents
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_2_KEY), DOCUMENT_2_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_1_AND_2);

		// Delete one document and check the result
		storageController.deleteDocumentAsync(ANY_USER, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_2_KEY), DOCUMENT_2_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_2);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER);
		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_2_KEY);
		verify(databaseWrapper).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_2);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addAndDeleteDocumentFailOnFirstDelete()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenAnswer(databaseFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_EMPTY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);

		// Check that user has no document and no metadata
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_EMPTY);

		// Add document and check the result
		storageController.addDocument(ANY_USER, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_1);

		// Delete document and check the result
		storageController.deleteDocumentAsync(ANY_USER, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER, DOCUMENT_1_KEY), null);
		assertEventually(() -> assertEquals(storageController.getDocumentMetadata(ANY_USER), DOCUMENT_METADATA_JSON_EMPTY));

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER);
		verify(databaseWrapper, times(3)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper, times(2)).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER, DOCUMENT_METADATA_JSON_EMPTY);
		verifyZeroInteractions(databaseWrapper);
	}
}
