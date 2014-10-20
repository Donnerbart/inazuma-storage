package de.donnerbart.inazuma.storage.cluster.storage;

import com.couchbase.client.core.BackpressureException;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class StorageControllerAddDocumentTest {
    private static final Observable<String> DATABASE_GET_RESPONSE_SUCCESS = Observable.just(null);
    private static final Observable<String> DATABASE_GET_RESPONSE_FAILURE = Observable.error(new BackpressureException());
    private static final Observable<Object> DATABASE_RESPONSE_SUCCESS = Observable.just(null);
    private static final Observable<Object> DATABASE_RESPONSE_FAILURE = Observable.error(new BackpressureException());

    private final static String ANY_USER_1 = "1000000";
    private final static String DOCUMENT_METADATA_KEY_USER_1 = "u-" + ANY_USER_1;
    private final static String ANY_USER_2 = "2000000";
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
	private final static String DOCUMENT_METADATA_JSON_2;
	private final static String DOCUMENT_METADATA_JSON_3;
	private final static String DOCUMENT_METADATA_JSON_1_AND_2;
	private final static String DOCUMENT_METADATA_JSON_2_AFTER_1;

    static {
        final Map<String, DocumentMetadata> documentMetadataMap1 = new HashMap<>();
        documentMetadataMap1.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
        DOCUMENT_METADATA_JSON_1 = GsonWrapper.toJson(documentMetadataMap1);

	    final Map<String, DocumentMetadata> documentMetadataMap2 = new HashMap<>();
	    documentMetadataMap2.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
	    DOCUMENT_METADATA_JSON_2 = GsonWrapper.toJson(documentMetadataMap2);

	    final Map<String, DocumentMetadata> documentMetadataMap3 = new HashMap<>();
	    documentMetadataMap3.put(DOCUMENT_3_KEY, DOCUMENT_3_METADATA);
	    DOCUMENT_METADATA_JSON_3 = GsonWrapper.toJson(documentMetadataMap3);

        final Map<String, DocumentMetadata> documentMetadataMap1And2 = new HashMap<>();
        documentMetadataMap1And2.put(DOCUMENT_1_KEY, DOCUMENT_1_METADATA);
        documentMetadataMap1And2.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
        DOCUMENT_METADATA_JSON_1_AND_2 = GsonWrapper.toJson(documentMetadataMap1And2);

        final Map<String, DocumentMetadata> documentMetadataMap2After1 = GsonWrapper.getDocumentMetadataMap(DOCUMENT_METADATA_JSON_1);
        documentMetadataMap2After1.put(DOCUMENT_2_KEY, DOCUMENT_2_METADATA);
        DOCUMENT_METADATA_JSON_2_AFTER_1 = GsonWrapper.toJson(documentMetadataMap2After1);
    }

	private int failureCount;
	private Answer<Observable<?>> failOnceThenSuccessDatabaseGetResponse;
	private Answer<Observable<?>> failOnceThenSuccessDatabaseResponse;

    @Mock
    private DatabaseWrapper databaseWrapper;

    private StorageController storageController;

    @BeforeMethod
    public void setUp() throws Exception {
	    failureCount = 0;
	    failOnceThenSuccessDatabaseGetResponse = invocationOnMock -> {
		    if (failureCount++ == 0)
		    {
			    return DATABASE_GET_RESPONSE_FAILURE;
		    }
		    return DATABASE_GET_RESPONSE_SUCCESS;
	    };
	    failOnceThenSuccessDatabaseResponse = invocationOnMock -> {
		    if (failureCount++ == 0)
		    {
			    return DATABASE_RESPONSE_FAILURE;
		    }
		    return DATABASE_RESPONSE_SUCCESS;
	    };

        MockitoAnnotations.initMocks(this);

        storageController = new StorageController(databaseWrapper, 0);
    }

    @AfterMethod
    public void tearDown() {
        StatisticManager.getInstance().shutdown();
    }

    @Test
    public void addFirstDocument() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.shutdown();

        verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
        verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verifyZeroInteractions(databaseWrapper);
    }

    @Test
    public void addSecondDocumentAfterFirstDocumentIsAlreadyPersisted() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(Observable.just(DOCUMENT_METADATA_JSON_1));
        when(databaseWrapper.insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2_AFTER_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

        storageController.addDocument(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
        storageController.shutdown();

        verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper).insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON);
        verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2_AFTER_1);
        verifyZeroInteractions(databaseWrapper);
    }

    @Test
    public void addSameDocumentTwice() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.shutdown();

        verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
        verify(databaseWrapper, atLeastOnce()).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verify(databaseWrapper, atMost(2)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verifyZeroInteractions(databaseWrapper);
    }

    @Test
    public void addSingleDocumentWithFailureOnFirstDatabaseSet() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenAnswer(failOnceThenSuccessDatabaseResponse);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.shutdown();

        verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
        verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verifyZeroInteractions(databaseWrapper);
    }

    @Test
    public void persistDocumentMetadataWithFailureOnFirstDatabaseSet() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(failOnceThenSuccessDatabaseResponse);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.shutdown();

        verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
        verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verifyZeroInteractions(databaseWrapper);
    }

    @Test
    public void persistDocumentMetadataWithFailureOnFirstAndSecondDatabaseSet() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(failOnceThenSuccessDatabaseResponse);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.shutdown();

        verify(databaseWrapper).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
	    verify(databaseWrapper, times(2)).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verifyZeroInteractions(databaseWrapper);
    }

    @Test
    public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromSameUser() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_2_KEY, DOCUMENT_2_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(failOnceThenSuccessDatabaseResponse);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_2)).thenAnswer(failOnceThenSuccessDatabaseResponse);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1_AND_2)).thenAnswer(failOnceThenSuccessDatabaseResponse);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.addDocument(ANY_USER_1, DOCUMENT_2_KEY, DOCUMENT_2_JSON, DOCUMENT_2_CREATED);
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
    public void addDocumentMetadataWithFailureOnFirstDatabaseSetWithSecondDocumentOnQueueFromDifferentUser() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_2)).thenReturn(DATABASE_GET_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_3_KEY, DOCUMENT_3_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenAnswer(failOnceThenSuccessDatabaseResponse);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_2, DOCUMENT_METADATA_JSON_3)).thenAnswer(failOnceThenSuccessDatabaseResponse);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.addDocument(ANY_USER_2, DOCUMENT_3_KEY, DOCUMENT_3_JSON, DOCUMENT_3_CREATED);
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
    public void getDocumentMetadataFailureOnFirstDatabaseGet() {
        when(databaseWrapper.getDocument(DOCUMENT_METADATA_KEY_USER_1)).thenAnswer(failOnceThenSuccessDatabaseGetResponse);
        when(databaseWrapper.insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON)).thenReturn(DATABASE_RESPONSE_SUCCESS);
        when(databaseWrapper.insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1)).thenReturn(DATABASE_RESPONSE_SUCCESS);

        storageController.addDocument(ANY_USER_1, DOCUMENT_1_KEY, DOCUMENT_1_JSON, DOCUMENT_1_CREATED);
        storageController.shutdown();

        verify(databaseWrapper, times(2)).getDocument(DOCUMENT_METADATA_KEY_USER_1);
        verify(databaseWrapper).insertDocument(DOCUMENT_1_KEY, DOCUMENT_1_JSON);
        verify(databaseWrapper).insertDocument(DOCUMENT_METADATA_KEY_USER_1, DOCUMENT_METADATA_JSON_1);
        verifyZeroInteractions(databaseWrapper);
    }
}
