package com.example.application;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.Consume;
import akka.javasdk.annotations.Produce;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.consumer.Consumer;
import com.example.domain.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(id = "external-transaction-consumer")
@Consume.FromTopic("transaction-topic")
//@Consume.FromEventSourcedEntity(PositionEntity.class)
//@Produce.ToTopic("out")
//@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
public class ExternalTransactionConsumer extends Consumer {
    private static final Logger logger = LoggerFactory.getLogger(ExternalTransactionConsumer.class);

    private final ComponentClient componentClient;

    public ExternalTransactionConsumer(ComponentClient componentClient) {
        this.componentClient = componentClient;
    }

    public Effect onTransaction(Transaction transaction) {
        logger.info("onTransaction: {}", transaction.id());
        var positionEntityId =  transaction.positionId().toEntityId("consumer");

        componentClient.forEventSourcedEntity(positionEntityId)
                .method(PositionEntity::processTransaction)
                .invoke(transaction);
        return effects().done();
    }
}
