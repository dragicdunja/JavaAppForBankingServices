/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package endpoints;

import Consts.Consts;
import entities.Client;
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
import javax.ws.rs.PATCH;
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

@Path("clients")
public class Clients {
    
    @Resource(lookup = "BankSystemCF")
    public ConnectionFactory cf;
    
    @Resource(lookup = "BankSubsystem1Queue")
    public Queue s1Queue;
    
    @Resource(lookup = "BankCentralServerQueue")
    public Queue bcsQueue;
    
    @GET
    public Response getAllClients() {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.GET_ALL_CLIENTS);
            
            producer.send(s1Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s1Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                ArrayList<Client> clients = (ArrayList<Client>)objMsg.getObject();
                return Response
                        .ok(new GenericEntity<ArrayList<Client>>(clients){})
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
        
        return Response.ok("error").build();
    }
    
    @POST
    public Response createNewClient(@QueryParam("name") String name, @QueryParam("address") String address, @QueryParam("idLocation") int idLocation) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.CREATE_NEW_CLIENT);
            objMsg.setStringProperty(Consts.NAME, name);
            objMsg.setStringProperty(Consts.ADDRESS, address);
            objMsg.setIntProperty(Consts.ID_LOCATION, idLocation);
            
            producer.send(s1Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s1Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Client created successfully.")
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
        
        
        return Response.ok("createNewClient ERROR").build();
    }
    
    @PATCH
    @Path("changeLocation/{IdClient}")
    public Response changeClientLocation(@PathParam("IdClient") int idClient, @QueryParam("idLocation") int idLocation) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.CHANGE_CLIENT_LOCATION);
            objMsg.setIntProperty(Consts.ID_CLIENT, idClient);
            objMsg.setIntProperty(Consts.ID_LOCATION, idLocation);
            
            producer.send(s1Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s1Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                return Response
                        .ok("Client location successfully changed.")
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
        
        return Response.ok("changeClientLocation working").build();
    }
    
}
