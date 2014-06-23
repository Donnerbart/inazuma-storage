package de.donnerbart.inazuma.storage.cluster.storage.metadata;

public class DocumentMetadataUtil
{
	public static String createKeyFromUserID(final String userID)
	{
		return "u-" + userID;
	}
}
