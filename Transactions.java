/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package endpoints;

import Consts.Consts;
import entities.Transaction;
import java.util.ArrayList;
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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;

/**
 *
 * @author jp170092d
 */
@Path("transactions")
public class Transactions {
    
    @Resource(lookup = "BankSystemCF")
    public ConnectionFactory cf;
    
    @Resource(lookup = "BankSubsystem2Queue")
    public Queue s2Queue;
    
    @Resource(lookup = "BankCentralServerQueue")
    public Queue bcsQueue;
    
    @POST
    @Path("transfer")
    public Response transfer(@QueryParam("idAccountFrom") int idAccountFrom, @QueryParam("idAccountTo") int idAccountTo, @QueryParam("amount") double amount, @QueryParam("purpose") String purpose, @QueryParam("idBranch") int idBranch) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.TRANSFER);
            objMsg.setIntProperty(Consts.ID_ACCOUNT_FROM, idAccountFrom);
            objMsg.setIntProperty(Consts.ID_ACCOUNT_TO, idAccountTo);
            objMsg.setDoubleProperty(Consts.AMOUNT, amount);
            objMsg.setStringProperty(Consts.PURPOSE, purpose);
            objMsg.setIntProperty(Consts.ID_BRANCH, idBranch);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Transfer completed successfully.")
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
             
        } catch (JMSException ex) {
            Logger.getLogger(Clients.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("transfer").build();
    }
    
    @POST
    @Path("deposit")
    public Response deposit(@QueryParam("idAccountTo") int idAccountTo, @QueryParam("amount") double amount, @QueryParam("purpose") String purpose, @QueryParam("idBranch") int idBranch) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.DEPOSIT);
            objMsg.setIntProperty(Consts.ID_ACCOUNT_TO, idAccountTo);
            objMsg.setDoubleProperty(Consts.AMOUNT, amount);
            objMsg.setStringProperty(Consts.PURPOSE, purpose);
            objMsg.setIntProperty(Consts.ID_BRANCH, idBranch);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Deposit completed successfully.")
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
             
        } catch (JMSException ex) {
            Logger.getLogger(Clients.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("deposit").build();
    }
    
    @POST
    @Path("withdraw")
    public Response withdraw(@QueryParam("idAccountFrom") int idAccountFrom, @QueryParam("amount") double amount, @QueryParam("purpose") String purpose, @QueryParam("idBranch") int idBranch) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.WITHDRAW);
            objMsg.setIntProperty(Consts.ID_ACCOUNT_FROM, idAccountFrom);
            objMsg.setDoubleProperty(Consts.AMOUNT, amount);
            objMsg.setStringProperty(Consts.PURPOSE, purpose);
            objMsg.setIntProperty(Consts.ID_BRANCH, idBranch);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Withdrawal completed successfully.")
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
             
        } catch (JMSException ex) {
            Logger.getLogger(Clients.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("withdraw").build();
    }
    
    @GET
    @Path("{idAccount}")
    public Response getAllAccountsTransactions(@PathParam("idAccount") int idAccount) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.GET_ALL_ACCOUNTS_TRANSACTIONS);
            objMsg.setIntProperty(Consts.ID_ACCOUNT, idAccount);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
//                ArrayList<Transaction> transactions = (ArrayList<Transaction>)objMsg.getObject();
                return Response
                        .ok(new GenericEntity<ArrayList<Transaction>>((ArrayList<Transaction>)objMsg.getObject()){})
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
             
        } catch (JMSException ex) {
            Logger.getLogger(Clients.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("getAllAccountsTransactions").build();
    }
    
}
