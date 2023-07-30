package com.smallworld;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TransactionDataFetcher {

    private List<Map<String, Object>> transactionalData;

    public TransactionDataFetcher(List<Map<String, Object>> transactionalData) {
        this.transactionalData = transactionalData;
    }

    /**
     * Returns the sum of the amounts of all transactions
     */
    public double getTotalTransactionAmount() {
        return transactionalData.stream()
                .mapToDouble(transaction -> (double) transaction.get("amount"))
                .sum();
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public double getTotalTransactionAmountSentBy(String senderName) {
        return transactionalData.stream()
                .filter(transaction -> senderName.equals(transaction.get("senderFullName")))
                .mapToDouble(transaction -> (double) transaction.get("amount"))
                .sum();
    }

    /**
     * Returns the highest transaction amount
     */
    public double getMaxTransactionAmount() {
        return transactionalData.stream()
                .mapToDouble(transaction -> (double) transaction.get("amount"))
                .max()
                .orElse(0.0);
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() {
        Set<String> uniqueCustomers = transactionalData.stream()
                .map(transaction -> (String) transaction.get("senderFullName"))
                .collect(Collectors.toSet());

        uniqueCustomers.addAll(transactionalData.stream()
                .map(transaction -> (String) transaction.get("beneficiaryFullName"))
                .collect(Collectors.toSet()));

        return uniqueCustomers.size();
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String customerName) {
        return transactionalData.stream()
                .filter(transaction -> customerName.equals(transaction.get("senderFullName"))
                        || customerName.equals(transaction.get("beneficiaryFullName")))
                .anyMatch(transaction -> !Boolean.TRUE.equals(transaction.get("issueSolved")));
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<String, List<Map<String, Object>>> getTransactionsByBeneficiaryName() {
        return transactionalData.stream()
                .collect(Collectors.groupingBy(transaction -> (String) transaction.get("beneficiaryFullName")));
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Integer> getUnsolvedIssueIds() {
        return transactionalData.stream()
                .filter(transaction -> !Boolean.TRUE.equals(transaction.get("issueSolved")))
                .map(transaction -> {
                    Object issueIdObject = transaction.get("issueId");
                    if (issueIdObject instanceof Integer) {
                        return (Integer) issueIdObject;
                    } else if (issueIdObject instanceof Double) {
                        return ((Double) issueIdObject).intValue();
                    } else {
                        // Handle the case when issueIdObject is not an Integer or Double.
                        // For example, you can throw an exception or return a default value.
                        throw new IllegalStateException("Invalid issueId type: " + issueIdObject.getClass().getName());
                    }
                })
                .collect(Collectors.toSet());
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() {
        return transactionalData.stream()
                .filter(transaction -> Boolean.TRUE.equals(transaction.get("issueSolved")))
                .map(transaction -> (String) transaction.get("issueMessage"))
                .collect(Collectors.toList());
    }

    /**
     * Returns the 3 transactions with highest amount sorted by amount descending
     */
    public List<Map<String, Object>> getTop3TransactionsByAmount() {
        return transactionalData.stream()
                .sorted((t1, t2) -> Double.compare((double) t2.get("amount"), (double) t1.get("amount")))
                .limit(3)
                .collect(Collectors.toList());
    }

    /**
     * Returns the sender with the most total sent amount
     */
    public Optional<String> getTopSender() {
        Map<String, Double> senderAmounts = transactionalData.stream()
                .collect(Collectors.groupingBy(
                        transaction -> (String) transaction.get("senderFullName"),
                        Collectors.summingDouble(transaction -> (double) transaction.get("amount"))
                ));

        return senderAmounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    public static void main(String[] args) throws IOException {

        String jsonContent = new String(Files.readAllBytes(Paths.get("transactions.json")));

        // System.out.println(jsonContent);

        Gson gson = new Gson();
        List<Map<String, Object>> transactions = gson.fromJson(jsonContent, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
        TransactionDataFetcher dataFetcher = new TransactionDataFetcher(transactions);

        System.out.println("Total Transaction Amount: " + dataFetcher.getTotalTransactionAmount());
        System.out.println("Total Transaction Amount Sent by Aunt Polly: " + dataFetcher.getTotalTransactionAmountSentBy("Aunt Polly"));
        System.out.println("Max Transaction Amount: " + dataFetcher.getMaxTransactionAmount());
        System.out.println("Count Unique Clients: " + dataFetcher.countUniqueClients());
        System.out.println("Has Open Compliance Issues for Tom Shelby: " + dataFetcher.hasOpenComplianceIssues("Tom Shelby"));
        System.out.println("Transactions by Beneficiary Name: " + dataFetcher.getTransactionsByBeneficiaryName());
        System.out.println("Unsolved Issue IDs: " + dataFetcher.getUnsolvedIssueIds());
        System.out.println("All Solved Issue Messages: " + dataFetcher.getAllSolvedIssueMessages());
        System.out.println("Top 3 Transactions by Amount: " + dataFetcher.getTop3TransactionsByAmount());
        System.out.println("Top Sender: " + dataFetcher.getTopSender().orElse("No top sender found."));
    }

}
