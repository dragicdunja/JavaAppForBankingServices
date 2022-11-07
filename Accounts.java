/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package endpoints;

import Consts.Consts;
import entities.Account;
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
import javax.ws.rs.DELETE;
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

@Path("accounts")
public class Accounts {
    
    @Resource(lookup = "BankSystemCF")
    public ConnectionFactory cf;
    
    @Resource(lookup = "BankSubsystem2Queue")
    public Queue s2Queue;
    
    @Resource(lookup = "BankCentralServerQueue")
    public Queue bcsQueue;
    
    @POST
    public Response createAccount(@QueryParam("idClient") int idClient, @QueryParam("idLocation") int idLocation, @QueryParam("allowedCredit") double allowedCredit) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.CREATE_NEW_ACCOUNT);
            objMsg.setIntProperty(Consts.ID_CLIENT, idClient);
            objMsg.setIntProperty(Consts.ID_LOCATION, idLocation);
            objMsg.setDoubleProperty(Consts.ALLOWED_CREDIT, allowedCredit);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Account created successfully.")
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
            
        } catch (JMSException ex) {
            Logger.getLogger(Locations.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("createLocation working").build();
    }
    
    @GET
    @Path("{idClient}")
    public Response getClientAccounts(@PathParam("idClient") int idClient) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        System.err.println(idClient);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.GET_CLIENT_ACCOUNTS);
            objMsg.setIntProperty(Consts.ID_CLIENT, idClient);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                ArrayList<Account> accounts = (ArrayList<Account>)objMsg.getObject();
                return Response
                        .ok(new GenericEntity<ArrayList<Account>>(accounts){})
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
            
        } catch (JMSException ex) {
            Logger.getLogger(Locations.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("ERROR").build();
    }
    
    @DELETE
    @Path("delete/{idAccount}")
    public Response deleteAccount(@PathParam("idAccount") int idAccount) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.DELETE_ACCOUNT);
            objMsg.setIntProperty(Consts.ID_ACCOUNT, idAccount);
            
            producer.send(s2Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s2Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Account deleted successfully.")
                        .build();
            } else {
                return Response
                        .status(Response.Status.CONFLICT)
                        .entity(objMsg.getStringProperty(Consts.MESSAGE))
                        .build();
            }
            
        } catch (JMSException ex) {
            Logger.getLogger(Locations.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return Response.ok("deleteAccount").build();
    }
    
}
