package de.donnerbart.inazuma.storage.base.request.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import de.donnerbart.inazuma.storage.base.request.task.AddDocumentTask;

import java.io.IOException;

public class AddDocumentTaskStreamSerializer implements StreamSerializer<AddDocumentTask>
{
	@Override
	public void write(final ObjectDataOutput out, final AddDocumentTask object) throws IOException
	{
		out.writeUTF(object.getUserID());
		out.writeUTF(object.getKey());
		out.writeUTF(object.getJson());
		out.writeLong(object.getCreated());
	}

	@Override
	public AddDocumentTask read(final ObjectDataInput in) throws IOException
	{
		return new AddDocumentTask(in.readUTF(), in.readUTF(), in.readUTF(), in.readLong());
	}

	@Override
	public int getTypeId()
	{
		return StreamSerializerId.ADD_DOCUMENT_TASK.ordinal();
	}

	@Override
	public void destroy()
	{
	}
}
