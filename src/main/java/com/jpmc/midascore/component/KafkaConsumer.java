package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    @Autowired
    private DatabaseConduit databaseConduit;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-consumer-group")
    public void listen(Transaction transaction){
        logger.info("Received transaction: {}", transaction);
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        // Check if the transaction is valid
        if(senderId <= 0 || recipientId <= 0 || amount <= 0) {
            logger.warn("Invalid transaction received: {}", transaction);
            return;
        }

        // Fetch sender and recipient from the database
        UserRecord sender = databaseConduit.findUserById(senderId);
        UserRecord recipient = databaseConduit.findUserById(recipientId);

        // Validate sender and recipient
        if(sender == null) {
            logger.warn("Sender not found for transaction: {}", transaction);
            return;
        }
        if(recipient == null) {
            logger.warn("Recipient not found for transaction: {}", transaction);
            return;
        }

        // Validate sender's balance
        if(sender.getBalance() < amount) {
            logger.warn("Insufficient balance for sender: {} in transaction: {}", senderId, transaction);
            return;
        }

        // Deduct the amount from sender's balance
        sender.setBalance(sender.getBalance() - amount);

        // Add the amount to recipient's balance
        recipient.setBalance(recipient.getBalance() + amount);

        logger.info("Updated balances — Sender: {}, Recipient: {}", sender.getBalance(), recipient.getBalance());

        // Create a new transaction record
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, amount);

        // Save the updated sender and recipient records
        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        // Save the transaction record
        databaseConduit.saveTransaction(transactionRecord);

        logger.info("Transaction processed successfully: {}", transactionRecord);
    }
}
