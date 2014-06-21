package de.donnerbart.inazuma.storage.cluster.storage;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.StringDocument;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class StorageControllerAddDocumentTest
{
	private final static String ANY_USER_1 = "1000000";
	private final static String ANY_USER_2 = "2000000";

	private final static String DOCUMENT_METADATA_KEY_USER_1 = "u-" + ANY_USER_1;
	private final static String DOCUMENT_METADATA_KEY_USER_2 = "u-" + ANY_USER_2;

	private final static String DOCUMENT_1_KEY = "11111111-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_1_JSON = "{content:\"mail1\"}";
	private final static long DOCUMENT_1_CREATED = 123456789;
	private final static DocumentMetadata DOCUMENT_1_METADATA = new DocumentMetadata(DOCUMENT_1_CREATED, false);

	private final static String DOCUMENT_2_KEY = "22222222-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_2_JSON = "{content:\"mail2\"}";
	private final static long DOCUMENT_2_CREATED = 234567891;
	private final static DocumentMetadata DOCUMENT_2_METADATA = new DocumentMetadata(DOCUMENT_2_CREATED, false);

	private final static String DOCUMENT_3_KEY = "33333333-e786-485e-963c-9a613b0368fe";
	private final static String DOCUMENT_3_JSON = "{content:\"mail3\"}";
	private final static long DOCUMENT_3_CREATED = 345678912;
	private final static DocumentMetadata DOCUMENT_3_METADATA = new DocumentMetadata(DOCUMENT_3_CREATED, false);

	private final static String DOCUMENT_METADATA_JSON_1;
	private final static String DOCUMENT_METADATA_JSON_1_AND_2;
	private final static String DOCUMENT_METADATA_JSON_2_AFTER_1;
	private final static String DOCUMENT_METADATA_JSON_3;

	static
	{
		final Map<String, DocumentMetadata> documentMetadataMap1 = new HashMap<>();
		documentMetadataMap1.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
		DOCUMENT_METADATA_JSON_1 = GsonWrapper.toJson(documentMetadataMap1);

		final Map<String, DocumentMetadata> documentMetadataMap2 = new HashMap<>();
		documentMetadataMap2.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
		documentMetadataMap2.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
		DOCUMENT_METADATA_JSON_1_AND_2 = GsonWrapper.toJson(documentMetadataMap2);

		final Map<String, DocumentMetadata> documentMetadataMap = GsonWrapper.getDocumentMetadataMap(DOCUMENT_METADATA_JSON_1);
		documentMetadataMap.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
		DOCUMENT_METADATA_JSON_2_AFTER_1 = GsonWrapper.toJson(documentMetadataMap);

		final Map<String, DocumentMetadata> documentMetadataMap3 = new HashMap<>();
		documentMetadataMap3.put(DOCUMENT_3_KEY, DOCUMENT_3_METADATA);
		DOCUMENT_METADATA_JSON_3 = GsonWrapper.toJson(documentMetadataMap3);
	}

	private Bucket bucket;

	private StorageController storageController;

	@BeforeMethod
	@SuppressWarnings("unchecked")
	public void setUp() throws Exception
	{
		bucket = mock(Bucket.class, RETURNS_DEEP_STUBS);

		MockitoAnnotations.initMocks(this);

		storageController = new StorageController(bucket);
	}

	@AfterMethod
	public void tearDown()
	{
		StatisticManager.getInstance().shutdown();
	}

	@Test
	public void addFirstDocument()
	{
		//when(bucket.upsert(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON).toBlockingObservable().single()).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1, StringDocument.class).toBlocking().single().content()).thenReturn(null);
		//when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		//verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		//verify(bucket).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	/*
	@Test
	public void addSecondDocumentAfterFirstDocumentIsAlreadyPersisted()
	{
		when(bucket.set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DOCUMENT_METADATA_JSON_1);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_2_AFTER_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_2_AFTER_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void addSameDocumentTwice()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket, times(2)).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void addSingleDocumentWithExceptionOnFirstDatabaseSet()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenThrow(new IllegalStateException()).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket, times(2)).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void addSingleDocumentWithFailureOnFirstDatabaseSet()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureFalse).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket, times(2)).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void persistDocumentMetadataWithExceptionOnFirstDatabaseSet()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenThrow(new IllegalStateException()).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void persistDocumentMetadataWithFailureOnFirstDatabaseSet()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureFalse).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void persistDocumentMetadataWithFailureOnFirstAndSecondDatabaseSet()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureFalse).thenReturn(futureFalse).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket, times(3)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromSameUser()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1_AND_2)).thenReturn(futureFalse).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1_AND_2);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromDifferentUser()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.set(DOCUMENT_3_KEY, 0, DOCUMENT_3_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_2)).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureFalse).thenReturn(futureTrue);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_2, 0, DOCUMENT_METADATA_JSON_3)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocumentAsync(ANY_USER_2, DOCUMENT_3_KEY, DOCUMENT_3_JSON, DOCUMENT_3_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket).set(DOCUMENT_3_KEY, 0, DOCUMENT_3_JSON);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket).get(DOCUMENT_METADATA_KEY_USER_2);
		verify(bucket, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verify(bucket).set(DOCUMENT_METADATA_KEY_USER_2, 0, DOCUMENT_METADATA_JSON_3);
		verifyZeroInteractions(bucket);
	}

	@Test
	public void getDocumentMetadataExceptionOnFirstDatabaseGet()
	{
		when(bucket.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(bucket.get(DOCUMENT_METADATA_KEY_USER_1)).thenThrow(new OperationTimeoutException("Operation timeout")).thenReturn(null);
		when(bucket.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(bucket).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(bucket, times(2)).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(bucket).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(bucket);
	}
	*/
}
