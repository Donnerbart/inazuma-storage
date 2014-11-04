package de.donnerbart.inazuma.storage.benchmark.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class ActorFactory
{
	public static ActorRef createMessageDispatcher(final ActorSystem context, final AddDocumentParameters parameters)
	{
		return context.actorOf(Props.create(
				AddDocument.class,
				() -> new AddDocument(parameters)
		));
	}
}
