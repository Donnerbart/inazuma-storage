package request;

import com.hazelcast.core.PartitionAware;
import main.Main;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class GetKeysFromLookupTask implements Callable<String>, PartitionAware, Serializable
{
	private final String userID;

	public GetKeysFromLookupTask(final String userID)
	{
		this.userID = userID;
	}

	@Override
	public String call() throws Exception
	{
		return Main.getStorageController().getKeysByUserID(userID);
	}

	@Override
	public Object getPartitionKey()
	{
		return userID;
	}
}
