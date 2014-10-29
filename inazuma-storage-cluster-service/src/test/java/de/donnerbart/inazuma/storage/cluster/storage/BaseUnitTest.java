package de.donnerbart.inazuma.storage.cluster.storage;

import com.couchbase.client.core.BackpressureException;
import de.donnerbart.inazuma.storage.base.stats.StatisticManager;
import de.donnerbart.inazuma.storage.cluster.storage.metadata.DocumentMetadata;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.DatabaseWrapper;
import de.donnerbart.inazuma.storage.cluster.storage.wrapper.GsonWrapper;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;

public abstract class BaseUnitTest
{
	static final Observable<String> DATABASE_GET_RESPONSE_SUCCESS = Observable.just(null);
	static final Observable<String> DATABASE_GET_RESPONSE_FAILURE = Observable.error(new BackpressureException());
	static final Observable<Boolean> DATABASE_RESPONSE_SUCCESS = Observable.just(Boolean.TRUE);
	static final Observable<Boolean> DATABASE_RESPONSE_FAILURE = Observable.error(new BackpressureException());

	final static String ANY_USER_1 = "1000000";
	final static String DOCUMENT_METADATA_KEY_USER_1 = "u-" + ANY_USER_1;
	final static String ANY_USER_2 = "2000000";
	final static String DOCUMENT_METADATA_KEY_USER_2 = "u-" + ANY_USER_2;

	final static String DOCUMENT_METADATA_JSON_EMPTY = "{}";

	final static String DOCUMENT_1_KEY = "11111111-e786-485e-963c-9a613b0368fe";
	final static String DOCUMENT_1_JSON = "{content:\"mail1\"}";
	final static long DOCUMENT_1_CREATED = 123456789;
	final static DocumentMetadata DOCUMENT_1_METADATA = new DocumentMetadata(DOCUMENT_1_CREATED, false);

	final static String DOCUMENT_2_KEY = "22222222-e786-485e-963c-9a613b0368fe";
	final static String DOCUMENT_2_JSON = "{content:\"mail2\"}";
	final static long DOCUMENT_2_CREATED = 234567891;
	final static DocumentMetadata DOCUMENT_2_METADATA = new DocumentMetadata(DOCUMENT_2_CREATED, false);

	final static String DOCUMENT_3_KEY = "33333333-e786-485e-963c-9a613b0368fe";
	final static String DOCUMENT_3_JSON = "{content:\"mail3\"}";
	final static long DOCUMENT_3_CREATED = 345678912;
	final static DocumentMetadata DOCUMENT_3_METADATA = new DocumentMetadata(DOCUMENT_3_CREATED, false);

	final static String DOCUMENT_METADATA_JSON_1;
	final static String DOCUMENT_METADATA_JSON_2;
	final static String DOCUMENT_METADATA_JSON_3;
	final static String DOCUMENT_METADATA_JSON_1_AND_2;
	final static String DOCUMENT_METADATA_JSON_2_AFTER_1;

	static
	{
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

	@Mock
	DatabaseWrapper databaseWrapper;

	StorageController storageController;

	DatabaseFailureMultipleTimesAnswer<Boolean> databaseFailOnceAnswer;
	DatabaseFailureMultipleTimesAnswer<String> databaseGetFailOnceAnswer;

	@BeforeMethod
	public void setUp() throws Exception
	{
		MockitoAnnotations.initMocks(this);

		storageController = new StorageController(databaseWrapper, 0);

		databaseFailOnceAnswer = new DatabaseFailureMultipleTimesAnswer<>(DATABASE_RESPONSE_SUCCESS, DATABASE_RESPONSE_FAILURE);
		databaseGetFailOnceAnswer = new DatabaseFailureMultipleTimesAnswer<>(DATABASE_GET_RESPONSE_SUCCESS, DATABASE_GET_RESPONSE_FAILURE);
	}

	@AfterSuite
	public void tearDown()
	{
		StatisticManager.getInstance().shutdown();
	}
}