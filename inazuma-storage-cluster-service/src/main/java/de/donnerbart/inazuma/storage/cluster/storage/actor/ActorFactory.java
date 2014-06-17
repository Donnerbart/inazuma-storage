package de.donnerbart.inazuma.storage.cluster.storage.actor;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.Creator;
import de.donnerbart.inazuma.storage.cluster.storage.StorageController;

public class ActorFactory
{
	public static ActorRef createDeadLetterListener(final ActorSystem context)
	{
		return context.actorOf(Props.create(DeadLetterListener.class), "deadLetterListener");
	}

	public static ActorRef createMessageDispatcher(final ActorSystem context, final StorageController storageController)
	{
		return context.actorOf(Props.create(new Creator<MessageDispatcher>()
		{
			@Override
			public MessageDispatcher create() throws Exception
			{
				return new MessageDispatcher(storageController);
			}
		}), "messageDispatcher");
	}

	public static ActorRef createMessageProcessor(final ActorContext context, final StorageController storageController, final String userID)
	{
		return context.actorOf(Props.create(new Creator<MessageProcessor>()
		{
			@Override
			public MessageProcessor create() throws Exception
			{
				return new MessageProcessor(storageController, userID);
			}
		}));
	}

}
