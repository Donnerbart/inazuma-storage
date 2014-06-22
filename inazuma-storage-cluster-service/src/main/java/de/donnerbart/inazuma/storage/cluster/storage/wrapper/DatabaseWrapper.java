package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import rx.Observable;

public interface DatabaseWrapper
{
	public Observable<StringDocument> getDocument(final String id);

	public Observable<StringDocument> insertDocument(final String key, final String document);

	public Observable<JsonDocument> deleteDocument(final String id);
}
