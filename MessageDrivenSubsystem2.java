/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.banksubsystem2.resources;

import Consts.Consts;
import entities.Account;
import entities.Transaction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import resources.Bank2DatabaseData;

/**
 *
 * @author jp170092d
 */

@MessageDriven(activationConfig = {
    @ActivationConfigProperty(propertyName = "destinationLookup",
            propertyValue = "BankSubsystem2Queue"),
    @ActivationConfigProperty(propertyName = "destinationType",
            propertyValue = "javax.jms.Queue")
})
public class MessageDrivenSubsystem2 implements MessageListener {
    
    @PersistenceContext(name = "bank2PU")
    EntityManager em;
    
    @Resource(lookup = "BankSystemCF")
    public ConnectionFactory cf;
    
    @Resource(lookup = "BankCentralServerQueue")
    public Queue cbsQueue;
    
    @Resource(lookup = "BankSubsystem3Queue")
    public Queue s3Queue;
    
    @Override
    public void onMessage(Message message) {
        if(message instanceof ObjectMessage){
            try {
                ObjectMessage objMsg = (ObjectMessage) message;
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message received: " + objMsg.getStringProperty(Consts.ACTION));
                
                switch(objMsg.getStringProperty(Consts.ACTION)) {
                    case Consts.CREATE_NEW_ACCOUNT:
                        createNewAccount(objMsg.getIntProperty(Consts.ID_CLIENT), objMsg.getIntProperty(Consts.ID_LOCATION), objMsg.getDoubleProperty(Consts.ALLOWED_CREDIT));
                        break;
                    case Consts.GET_CLIENT_ACCOUNTS:
                        getClientAccounts(objMsg.getIntProperty(Consts.ID_CLIENT));
                        break;
                    case Consts.DELETE_ACCOUNT:
                        deleteAccount(objMsg.getIntProperty(Consts.ID_ACCOUNT));
                        break;
                        
                    case Consts.DEPOSIT:
                        deposit(objMsg.getIntProperty(Consts.ID_ACCOUNT_TO), objMsg.getDoubleProperty(Consts.AMOUNT), objMsg.getStringProperty(Consts.PURPOSE), objMsg.getIntProperty(Consts.ID_BRANCH));
                        break;
                    case Consts.WITHDRAW:
                        withdraw(objMsg.getIntProperty(Consts.ID_ACCOUNT_FROM), objMsg.getDoubleProperty(Consts.AMOUNT), objMsg.getStringProperty(Consts.PURPOSE), objMsg.getIntProperty(Consts.ID_BRANCH));
                        break;
                    case Consts.TRANSFER:
                        transfer(objMsg.getIntProperty(Consts.ID_ACCOUNT_FROM), objMsg.getIntProperty(Consts.ID_ACCOUNT_TO), objMsg.getDoubleProperty(Consts.AMOUNT), objMsg.getStringProperty(Consts.PURPOSE), objMsg.getIntProperty(Consts.ID_BRANCH));
                        break;
                    case Consts.GET_ALL_ACCOUNTS_TRANSACTIONS:
                        getAllAccountsTransactions(objMsg.getIntProperty(Consts.ID_ACCOUNT));
                        break;
                        
                    case Consts.FETCH_DATABASE:
                        fetchDatabase();
                        break;
                    case Consts.FETCH_DATABASE_DIFFERENCE:
                        fetchDatabaseDifference();
                        break;
                }
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void createNewAccount(int idClient, int idLocation, double allowedCredit) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        Account account = new Account();
        account.setIdClient(idClient);
        account.setIdLocation(idLocation);
        account.setDateTimeOfCreation(new Date());
        account.setAllowedCredit(allowedCredit);
        account.setBalance(0);
        account.setNumOfTransactions(0);
        account.setStatus('A');
        em.persist(account);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void getClientAccounts(int idClient) {
        
        List<Account> accounts = em.createQuery("SELECT a FROM Account a WHERE a.idClient = :idClient", Account.class).setParameter("idClient", idClient).getResultList();
        ArrayList<Account> response = new ArrayList<>();
        
        accounts.forEach((a) -> {
            response.add(a);
            System.out.println(a.getIdAccount());
        });
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(response);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void deleteAccount(int idAccount) {
        Account account = em.find(Account.class, idAccount);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(account == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No account with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        em.createQuery("DELETE FROM Account a WHERE a.idAccount = :idAccount").setParameter("idAccount", idAccount).executeUpdate();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void deposit(int idAccountTo, double amount, String purpose, int idBranch) {
        
        Account accountTo = em.find(Account.class, idAccountTo);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(accountTo == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No account with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        long transactionNo = em.createQuery("SELECT COUNT(t) + 1 FROM Transaction t WHERE t.idAccountTo = :idAccount OR t.idAccountFrom = :idAccount", Long.class)
                .setParameter("idAccount", accountTo)
                .getSingleResult();
        
        Transaction transaction = new Transaction();
        transaction.setDateTimeOfTransaction(new Date());
        transaction.setIdAccountTo(accountTo);
        transaction.setIdBranch(idBranch);
        transaction.setPurpose(purpose);
        transaction.setSerialNumberOnAccount((int)transactionNo);
        transaction.setSum(amount);
        transaction.setTransactionType('D');
        em.persist(transaction);
        
        
        accountTo.setBalance(accountTo.getBalance() + amount);
        accountTo.setNumOfTransactions(accountTo.getNumOfTransactions() + 1);
        if(accountTo.getStatus().equals('B') && accountTo.getBalance() + amount > -accountTo.getAllowedCredit())
            accountTo.setStatus('A');
        em.persist(accountTo);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void withdraw(int idAccountFrom, double amount, String purpose, int idBranch) {
        
        Account accountFrom = em.find(Account.class, idAccountFrom);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(accountFrom == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No account with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        if(accountFrom.getStatus().equals('B')) {
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "Account with the given ID is blocked.");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        long transactionNo = em.createQuery("SELECT COUNT(t) + 1 FROM Transaction t WHERE t.idAccountTo = :idAccount OR t.idAccountFrom = :idAccount", Long.class)
                .setParameter("idAccount", accountFrom)
                .getSingleResult();
        
        Transaction transaction = new Transaction();
        transaction.setDateTimeOfTransaction(new Date());
        transaction.setIdAccountFrom(accountFrom);
        transaction.setIdBranch(idBranch);
        transaction.setPurpose(purpose);
        transaction.setSerialNumberOnAccount((int)transactionNo);
        transaction.setSum(amount);
        transaction.setTransactionType('W');
        em.persist(transaction);
        
        
        accountFrom.setBalance(accountFrom.getBalance() - amount);
        accountFrom.setNumOfTransactions(accountFrom.getNumOfTransactions() + 1);
        if(accountFrom.getBalance() < -accountFrom.getAllowedCredit())
            accountFrom.setStatus('B');
        
        em.persist(accountFrom);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void transfer(int idAccountFrom, int idAccountTo, double amount, String purpose, int idBranch) {
        
        Account accountFrom = em.find(Account.class, idAccountFrom);
        Account accountTo = em.find(Account.class, idAccountTo);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(accountFrom == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No accountFrom with such ID " + idAccountFrom);
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        if(accountTo == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No accountTo with such ID " + idAccountTo);
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        if(accountFrom.getStatus().equals('B')) {
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "Account with the given ID is blocked.");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        long transactionNo = em.createQuery("SELECT COUNT(t) + 1 FROM Transaction t WHERE t.idAccountTo = :idAccount OR t.idAccountFrom = :idAccount", Long.class)
                .setParameter("idAccount", accountFrom)
                .getSingleResult();
        
        Transaction transaction = new Transaction();
        transaction.setDateTimeOfTransaction(new Date());
        transaction.setIdAccountFrom(accountFrom);
        transaction.setIdAccountTo(accountTo);
        transaction.setIdBranch(idBranch);
        transaction.setPurpose(purpose);
        transaction.setSerialNumberOnAccount((int)transactionNo);
        transaction.setSum(amount);
        transaction.setTransactionType('T');
        em.persist(transaction);
        
        
        accountFrom.setBalance(accountFrom.getBalance() - amount);
        accountFrom.setNumOfTransactions(accountFrom.getNumOfTransactions() + 1);
        if(accountFrom.getBalance() < -accountFrom.getAllowedCredit())
            accountFrom.setStatus('B');
        
        em.persist(accountFrom);
        
        accountTo.setBalance(accountTo.getBalance() + amount);
        accountTo.setNumOfTransactions(accountTo.getNumOfTransactions() + 1);
        if(accountTo.getStatus().equals('B') && accountTo.getBalance() + amount > -accountTo.getAllowedCredit())
            accountTo.setStatus('A');
        em.persist(accountTo);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void getAllAccountsTransactions(int idAccount) {
        
        List<Transaction> transactions = em.createQuery("SELECT t FROM Transaction t where t.idAccountFrom.idAccount = :idAccount OR t.idAccountTo.idAccount = :idAccount", Transaction.class).setParameter("idAccount", idAccount).getResultList();
        ArrayList<Transaction> response = new ArrayList<>();
        
        transactions.forEach((t) -> {
            response.add(t);
            System.out.println(t.getIdTransaction());
        });
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(response);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_2 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void fetchDatabase() {
        
        List<Account> accounts = em.createNamedQuery("Account.findAll", Account.class).getResultList();
        ArrayList<Account> responseAccounts = new ArrayList<>();
        List<Transaction> transactions = em.createNamedQuery("Transaction.findAll", Transaction.class).getResultList();
        ArrayList<Transaction> responseTransactions = new ArrayList<>();
        
        accounts.forEach((a) -> {
            responseAccounts.add(a);
        });
        transactions.forEach((t) -> {
            responseTransactions.add(t);
        });
        
        Bank2DatabaseData b2dd = new Bank2DatabaseData();
        b2dd.setAccounts(responseAccounts);
        b2dd.setTransactions(responseTransactions);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(b2dd);
            
            producer.send(s3Queue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void fetchDatabaseDifference() {
        List<Account> accounts = em.createNamedQuery("Account.findAll", Account.class).getResultList();
        ArrayList<Account> responseAccounts = new ArrayList<>();
        List<Transaction> transactions = em.createNamedQuery("Transaction.findAll", Transaction.class).getResultList();
        ArrayList<Transaction> responseTransactions = new ArrayList<>();
        
        accounts.forEach((a) -> {
            responseAccounts.add(a);
        });
        transactions.forEach((t) -> {
            responseTransactions.add(t);
        });
        
        Bank2DatabaseData b2dd = new Bank2DatabaseData();
        b2dd.setAccounts(responseAccounts);
        b2dd.setTransactions(responseTransactions);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_2);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(b2dd);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
