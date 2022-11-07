/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package endpoints;

import Consts.Consts;
import entities.Account;
import entities.Accountcopy;
import entities.Branch;
import entities.Branchcopy;
import entities.Client;
import entities.Clientcopy;
import entities.Location;
import entities.Locationcopy;
import entities.Transaction;
import entities.Transactioncopy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import resources.BackupDatabaseData;
import resources.Bank1DatabaseData;
import resources.Bank2DatabaseData;
import resources.CompleteDatabaseData;

/**
 *
 * @author jp170092d
 */

@Path("backup")
public class Backup {
    
    @Resource(lookup = "BankSystemCF")
    public ConnectionFactory cf;
    
    @Resource(lookup = "BankSubsystem1Queue")
    public Queue s1Queue;
    
    @Resource(lookup = "BankSubsystem2Queue")
    public Queue s2Queue;
    
    @Resource(lookup = "BankSubsystem3QueueMD")
    public Queue s3Queue;
    
    @Resource(lookup = "BankCentralServerQueue")
    public Queue bcsQueue;
    
    @Path("difference")
    @GET
    public Response getDatabaseDifference() {
        
        Bank1DatabaseData b1dd = fetchDatabase1();
        Bank2DatabaseData b2dd = fetchDatabase2();
        BackupDatabaseData bdd = fetchBackupDatabase();
        
        CompleteDatabaseData cdd = getBackupDifference(b1dd, b2dd, bdd);
        
        if(cdd != null) {
            return Response.ok(cdd).build(); 
        } else {
            return Response.noContent().build();
        }
        
    }
    
    @GET
    public Response getBackupDatabase() {
        
        BackupDatabaseData bdd = fetchBackupDatabase();
        
        if(bdd != null) {
            return Response.ok(bdd).build();
        } else {
            return Response.noContent().build();
        }
    }
    
    private Bank1DatabaseData fetchDatabase1() {
        JMSContext context = cf.createContext();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg1 = context.createObjectMessage();
        Bank1DatabaseData b1dd = null;
        
        try {
            
            objMsg1.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_3);
            objMsg1.setStringProperty(Consts.ACTION, Consts.FETCH_DATABASE_DIFFERENCE);
            
            producer.send(s1Queue, objMsg1);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s1Queue.getQueueName());
            objMsg1 = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg1.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                b1dd = (Bank1DatabaseData)objMsg1.getObject();
                
            } else {
                
                System.out.println("Error while fetching data from database Bank1");
                
            }
            
        } catch (JMSException ex) {
            Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return b1dd;
    }

    private Bank2DatabaseData fetchDatabase2() {
        JMSContext context = cf.createContext();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg1 = context.createObjectMessage();
        Bank2DatabaseData b2dd = null;
        
        try {
            
            objMsg1.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg1.setStringProperty(Consts.ACTION, Consts.FETCH_DATABASE_DIFFERENCE);
            
            producer.send(s2Queue, objMsg1);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg1 = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg1.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                b2dd = (Bank2DatabaseData)objMsg1.getObject();
                
            } else {
                
                System.out.println("Error while fetching data from database Bank2");
                
            }
            
        } catch (JMSException ex) {
            Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return b2dd;
    }
    
    private BackupDatabaseData fetchBackupDatabase() {
        JMSContext context = cf.createContext();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg1 = context.createObjectMessage();
        BackupDatabaseData bdd = null;
        
        try {
            
            objMsg1.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg1.setStringProperty(Consts.ACTION, Consts.FETCH_DATABASE_DIFFERENCE);
            
            producer.send(s3Queue, objMsg1);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s3Queue.getQueueName());
            objMsg1 = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg1.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                bdd = (BackupDatabaseData)objMsg1.getObject();
                
            } else {
                
                System.out.println("Error while fetching data from database Bank2");
                
            }
            
        } catch (JMSException ex) {
            Logger.getLogger(Backup.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return bdd;
    }

    private CompleteDatabaseData getBackupDifference(Bank1DatabaseData b1dd, Bank2DatabaseData b2dd, BackupDatabaseData bdd) {
        ArrayList<Location> locationDifference = new ArrayList<>();
        ArrayList<Client> clientDifference = new ArrayList<>();
        ArrayList<Branch> branchDifference = new ArrayList<>();
        ArrayList<Account> accountDifference = new ArrayList<>();
        ArrayList<Transaction> transactionDifference = new ArrayList<>();
        
        
        Iterator<Location> iterLocation = b1dd.getLocations().iterator();
        while(iterLocation.hasNext()) {
            Location l = iterLocation.next();
            boolean exists = false;
            Iterator<Locationcopy> iter = bdd.getLocations().iterator();
            Locationcopy next;
            while(iter.hasNext()) {
                if(Objects.equals(iter.next().getIdLocation(), l.getIdLocation())) {
                    exists = true;
                    break;
                }
                    
            }
            
            if(!exists)
                locationDifference.add(l);
        }
        
        Iterator<Client> iterClient = b1dd.getClients().iterator();
        while(iterClient.hasNext()) {
            Client c = iterClient.next();
            boolean exists = false;
            Iterator<Clientcopy> iter = bdd.getClients().iterator();
            Clientcopy next;
            while(iter.hasNext()) {
                if(Objects.equals(c.getIdClient(), iter.next().getIdClient())) {
                    exists = true;
                    break;
                }
                    
            }
            
            if(!exists)
                clientDifference.add(c);
        }
        
        Iterator<Branch> iterBranch = b1dd.getBranches().iterator();
        while(iterBranch.hasNext()) {
            Branch b = iterBranch.next();
            boolean exists = false;
            Iterator<Branchcopy> iter = bdd.getBranches().iterator();
            Branchcopy next;
            while(iter.hasNext()) {
                if(Objects.equals(iter.next().getIdBranch(), b.getIdBranch())) {
                    exists = true;
                    break;
                }
                    
            }
            
            if(!exists)
                branchDifference.add(b);
        }
        
        Iterator<Account> iterAccount = b2dd.getAccounts().iterator();
        while(iterAccount.hasNext()) {
            Account a = iterAccount.next();
            boolean exists = false;
            Iterator<Accountcopy> iter = bdd.getAccounts().iterator();
            Accountcopy next;
            while(iter.hasNext()) {
                if(Objects.equals(iter.next().getIdAccount(), a.getIdAccount())) {
                    exists = true;
                    break;
                }
                    
            }
            
            if(!exists)
                accountDifference.add(a);
        }
        
        Iterator<Transaction> iterTransaction = b2dd.getTransactions().iterator();
        while(iterTransaction.hasNext()) {
            Transaction t = iterTransaction.next();
            boolean exists = false;
            Iterator<Transactioncopy> iter = bdd.getTransactions().iterator();
            Transactioncopy next;
            while(iter.hasNext()) {
                if(Objects.equals(iter.next().getIdTransaction(), t.getIdTransaction())) {
                    exists = true;
                    break;
                } else {
                }
                    
            }
            
            if(!exists)
                transactionDifference.add(t);
        }
        
        CompleteDatabaseData cdd = new CompleteDatabaseData();
        cdd.setLocations(locationDifference);
        cdd.setClients(clientDifference);
        cdd.setBranches(branchDifference);
        cdd.setAccounts(accountDifference);
        cdd.setTransactions(transactionDifference);
        
        return cdd;
    }
    
}
