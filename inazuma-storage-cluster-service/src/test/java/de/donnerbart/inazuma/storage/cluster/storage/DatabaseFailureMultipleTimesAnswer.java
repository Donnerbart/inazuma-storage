package de.donnerbart.inazuma.storage.cluster.storage;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import rx.Observable;

class DatabaseFailureMultipleTimesAnswer<T> implements Answer<Observable<T>>
{
	private final Observable<T> successResponse;
	private final Observable<T> failureResponse;

	private int numberOfFailures;

	public DatabaseFailureMultipleTimesAnswer(final Observable<T> successResponse, final Observable<T> failureResponse)
	{
		this(successResponse, failureResponse, 1);
	}

	public DatabaseFailureMultipleTimesAnswer(final Observable<T> successResponse, final Observable<T> failureResponse, final int numberOfFailures)
	{
		this.successResponse = successResponse;
		this.failureResponse = failureResponse;

		this.numberOfFailures = numberOfFailures;
	}

	@Override
	public Observable<T> answer(final InvocationOnMock invocationOnMock) throws Throwable
	{
		if (numberOfFailures-- > 0)
		{
			return failureResponse;
		}
		return successResponse;
	}

	public void setNumberOfFailures(final int numberOfFailures)
	{
		this.numberOfFailures = numberOfFailures;
	}
}
