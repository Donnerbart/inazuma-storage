package de.donnerbart.inazuma.storage.cluster.storage;

import de.donnerbart.inazuma.storage.base.request.AddPersistenceLevel;
import org.testng.annotations.Test;
import rx.Observable;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static util.AssertionThread.assertEventually;

public class StorageControllerAddDocumentTest extends BaseUnitTest
{
	@Test
	public void addFirstDocument()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addSecondDocumentAfterFirstDocumentIsAlreadyPersisted()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(Observable.just(DOCUMENT_METADATA_JSON_1));
		when(databaseWrapper.insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2_AFTER_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		storageController.addDocument(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_2_AFTER_1);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2_AFTER_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addSameDocumentTwice()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper, atLeastOnce()).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper, atMost(2)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addSingleDocumentWithFailureOnFirstDatabaseSet()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenAnswer(databaseFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void persistDocumentMetadataWithFailureOnFirstDatabaseSet()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(databaseFailOnceAnswer);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void persistDocumentMetadataWithFailureOnFirstAndSecondDatabaseSet()
	{
		databaseFailOnceAnswer.setNumberOfFailures(2);

		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(databaseFailOnceAnswer);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper, times(3)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void persistDocumentMetadataWithFailureOnFirstAndSecondDatabaseSetAssertEventually()
	{
		databaseFailOnceAnswer.setNumberOfFailures(2);

		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(databaseFailOnceAnswer);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED, AddPersistenceLevel.REQUEST_ON_QUEUE);
		assertEventually(() -> assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1));

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper, times(3)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromSameUser()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(databaseFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2)).thenAnswer(databaseFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1_AND_2)).thenAnswer(databaseFailOnceAnswer);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocument(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1_AND_2);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON);
		verify(databaseWrapper, atMost(1)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper, atMost(1)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2);
		verify(databaseWrapper, atMost(1)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1_AND_2);
		verify(databaseWrapper, times(2)).insertDocument(eq(DOCUMENT_METADATA_KEY_USER_1), anyString());
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromDifferentUser()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_2)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_3_KEY, DOCUMENT_3_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(databaseFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_2, DOCUMENT_METADATA_JSON_3)).thenAnswer(databaseFailOnceAnswer);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocument(ANY_USER_2, DOCUMENT_3_KEY, DOCUMENT_3_JSON, DOCUMENT_3_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_2), DOCUMENT_METADATA_JSON_3);

		storageController.shutdown();

		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_2);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_3_KEY, DOCUMENT_3_JSON);
		verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_2, DOCUMENT_METADATA_JSON_3);
		verifyZeroInteractions(databaseWrapper);
	}

	@Test
	public void getDocumentMetadataFailureOnFirstDatabaseGet()
	{
		when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenAnswer(databaseGetFailOnceAnswer);
		when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
		when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

		storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		assertEquals(storageController.getDocumentMetadata(ANY_USER_1), DOCUMENT_METADATA_JSON_1);

		storageController.shutdown();

		verify(databaseWrapper, times(2)).getDocument(DOCUMENT_METADATA_KEY_USER_1);
		verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
		verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(databaseWrapper);
	}
}
