package de.donnerbart.inazuma.storage.cluster.storage;

import org.testng.annotations.Test;
import rx.Observable;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

public class StorageControllerDeleteDocumentTest extends BaseUnitTest
{
	@Test
	public void addAndDeleteDocument()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_EMPTY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);

		// Check that user has no document and no metadata
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_EMPTY);

		// Add document and check the result
		addDocumentAndWait(storageController, ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		// Delete document and check the result
		storageController.deleteDocument(ANY_USER_1, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_EMPTY);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper, times(3)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_EMPTY);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void deleteAlreadyPersistedDocument()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(Observable.just(DOCUMENT_METADATA_JSON_1));
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_EMPTY)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		// Check that user has one document and metadata for this document
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		// Delete document and check the result
		storageController.deleteDocument(ANY_USER_1, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_EMPTY);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_EMPTY);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void deleteOneOfTwoAlreadyPersistedDocuments()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(Observable.just(DOCUMENT_METADATA_JSON_1_AND_2));
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_2_KEY)).thenReturn(Observable.just(DOCUMENT_2_JSON));
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		// Check that user has two documents and metadata for those documents
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_2_KEY), DOCUMENT_2_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1_AND_2);

		// Delete one document and check the result
		storageController.deleteDocument(ANY_USER_1, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_2_KEY), DOCUMENT_2_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_2);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_2_KEY);
		verify(databaseWrapper).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addAndDeleteDocumentFailOnFirstDelete()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.deleteDocument(DOCUMENT_1_KEY)).thenAnswer(databaseFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_EMPTY)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_1_KEY)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS).thenReturn(Observable.just(DOCUMENT_1_JSON)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);

		// Check that user has no document and no metadata
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_EMPTY);

		// Add document and check the result
		addDocumentAndWait(storageController, ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), DOCUMENT_1_JSON);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		// Delete document and check the result
		storageController.deleteDocument(ANY_USER_1, DOCUMENT_1_KEY);
		assertEquals(storageController.getDocument(ANY_USER_1, DOCUMENT_1_KEY), null);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_EMPTY);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper, times(3)).getDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper, times(2)).deleteDocument(DOCUMENT_1_KEY);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_EMPTY);
		verifyZeroInteractions(databaseWrapper);
	}
}
