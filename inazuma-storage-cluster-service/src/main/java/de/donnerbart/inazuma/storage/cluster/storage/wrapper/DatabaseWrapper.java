package de.donnerbart.inazuma.storage.cluster.storage.wrapper;

import de.donnerbart.inazuma.storage.cluster.storage.wrapper.response.DatabaseResponse;
import rx.Observable;

public interface DatabaseWrapper
{
	public Observable<DatabaseResponse> getDocument(final String id);

	public Observable<DatabaseResponse> insertDocument(final String key, final String document);

	public Observable<DatabaseResponse> deleteDocument(final String id);
}
