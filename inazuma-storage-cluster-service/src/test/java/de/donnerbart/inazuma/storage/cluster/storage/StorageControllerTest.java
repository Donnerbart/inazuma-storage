package de.donnerbart.inazuma.storage.cluster.storage;

import com.couchbase.client.CouchbaseClient;
import com.hazelcast.core.OperationTimeoutException;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.model.DocumentMetadata;
import net.spy.memcached.internal.OperationFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class StorageControllerTest
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
		DOCUMENT_METADATA_JSON_1 = StorageJsonController.toJson(documentMetadataMap1);

		final Map<String, DocumentMetadata> documentMetadataMap2 = new HashMap<>();
		documentMetadataMap2.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
		documentMetadataMap2.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
		DOCUMENT_METADATA_JSON_1_AND_2 = StorageJsonController.toJson(documentMetadataMap2);

		final Map<String, DocumentMetadata> documentMetadataMap = StorageJsonController.getDocumentMetadataMap(DOCUMENT_METADATA_JSON_1);
		documentMetadataMap.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
		DOCUMENT_METADATA_JSON_2_AFTER_1 = StorageJsonController.toJson(documentMetadataMap);

		final Map<String, DocumentMetadata> documentMetadataMap3 = new HashMap<>();
		documentMetadataMap3.put(DOCUMENT_3_KEY, DOCUMENT_3_METADATA);
		DOCUMENT_METADATA_JSON_3 = StorageJsonController.toJson(documentMetadataMap3);
	}

	private CouchbaseClient client;
	private StorageController storageController;

	@Mock
	private OperationFuture<Boolean> futureTrue;
	@Mock
	private OperationFuture<Boolean> futureFalse;

	@BeforeMethod
	public void setUp() throws Exception
	{
		client = mock(CouchbaseClient.class);
		storageController = new StorageController(client);

		MockitoAnnotations.initMocks(this);
		when(futureTrue.get()).thenReturn(true);
		when(futureFalse.get()).thenReturn(false);
	}

	@AfterMethod
	public void tearDown()
	{
		StatisticManager.getInstance().shutdown();
	}

	@Test(timeOut = 1000)
	public void addFirstDocumentForUser()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addSecondDocumentAfterFirstDocumentIsAlreadyPersisted()
	{
		when(client.set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DOCUMENT_METADATA_JSON_1);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_2_AFTER_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_2_AFTER_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addSameDocumentTwice()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client, times(2)).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addSingleDocumentWithExceptionOnFirstDatabaseSet()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenThrow(new IllegalStateException()).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client, times(2)).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addSingleDocumentWithFailureOnFirstDatabaseSet()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureFalse).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client, times(2)).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistDocumentMetadataWithExceptionOnFirstDatabaseSet()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenThrow(new IllegalStateException()).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistDocumentMetadataWithFailureOnFirstDatabaseSet()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureFalse).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void persistDocumentMetadataWithFailureOnFirstAndSecondDatabaseSet()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureFalse).thenReturn(futureFalse).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client, times(3)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromSameUser()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1_AND_2)).thenReturn(futureFalse).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).set(DOCUMENT_2_KEY, 0, DOCUMENT_2_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1_AND_2);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromDifferentUser()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.set(DOCUMENT_3_KEY, 0, DOCUMENT_3_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(null);
		when(client.get(DOCUMENT_METADATA_KEY_USER_2)).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureFalse).thenReturn(futureTrue);
		when(client.set(DOCUMENT_METADATA_KEY_USER_2, 0, DOCUMENT_METADATA_JSON_3)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.addDocumentAsync(ANY_USER_2, DOCUMENT_3_KEY, DOCUMENT_3_JSON, DOCUMENT_3_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client).set(DOCUMENT_3_KEY, 0, DOCUMENT_3_JSON);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).get(DOCUMENT_METADATA_KEY_USER_2);
		verify(client, times(2)).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_2, 0, DOCUMENT_METADATA_JSON_3);
		verifyZeroInteractions(client);
	}

	@Test(timeOut = 1000)
	public void getDocumentMetadataExceptionOnFirstDatabaseGet()
	{
		when(client.set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON)).thenReturn(futureTrue);
		when(client.get(DOCUMENT_METADATA_KEY_USER_1)).thenThrow(new OperationTimeoutException("Operation timeout")).thenReturn(null);
		when(client.set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1)).thenReturn(futureTrue);

		storageController.addDocumentAsync(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
		storageController.shutdown();
		storageController.awaitShutdown();

		verify(client).set(DOCUMENT_1_KEY, 0, DOCUMENT_1_JSON);
		verify(client, times(2)).get(DOCUMENT_METADATA_KEY_USER_1);
		verify(client).set(DOCUMENT_METADATA_KEY_USER_1, 0, DOCUMENT_METADATA_JSON_1);
		verifyZeroInteractions(client);
	}
}
