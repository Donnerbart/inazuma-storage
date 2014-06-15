package request.serialization;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import request.task.GetDocumentTask;

import java.io.IOException;

public class GetDocumentTaskStreamSerializer implements StreamSerializer<GetDocumentTask>
{
	@Override
	public void write(final ObjectDataOutput out, final GetDocumentTask object) throws IOException
	{
		out.writeUTF(object.getUserID());
		out.writeUTF(object.getKey());
	}

	@Override
	public GetDocumentTask read(final ObjectDataInput in) throws IOException
	{
		return new GetDocumentTask(in.readUTF(), in.readUTF());
	}

	@Override
	public int getTypeId()
	{
		return StreamSerializerId.GET_DOCUMENT_TASK.ordinal();
	}

	@Override
	public void destroy()
	{
	}
}
