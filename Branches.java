/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package endpoints;

import Consts.Consts;
import entities.Branch;
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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;

/**
 *
 * @author jp170092d
 */

@Path("branches")
public class Branches {
    
    @Resource(lookup = "BankSystemCF")
    public ConnectionFactory cf;
    
    @Resource(lookup = "BankSubsystem1Queue")
    public Queue s1Queue;
    
    @Resource(lookup = "BankCentralServerQueue")
    public Queue bcsQueue;
    
    @GET
    public Response getAllBranches() {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.GET_ALL_BRANCHES);
            
            producer.send(s1Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s1Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                ArrayList<Branch> branches = (ArrayList<Branch>)objMsg.getObject();
                return Response
                        .ok(new GenericEntity<ArrayList<Branch>>(branches){})
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
    public Response createBranch(@QueryParam("address") String address, @QueryParam("idLocation") int idLocation) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        JMSConsumer consumer = context.createConsumer(bcsQueue);
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.CENTRAL_BANK_SERVER);
            objMsg.setStringProperty(Consts.ACTION, Consts.CREATE_NEW_BRANCH);
            objMsg.setStringProperty(Consts.ADDRESS, address);
            objMsg.setIntProperty(Consts.ID_LOCATION, idLocation);
            
            producer.send(s1Queue, objMsg);
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message sent " + s1Queue.getQueueName());
            objMsg = (ObjectMessage)consumer.receive();
            System.out.println(Consts.CENTRAL_BANK_SERVER + ": Message received");
            
            if(objMsg.getStringProperty(Consts.RESULT).equals(Consts.SUCCESS)) {
                
                return Response
                        .ok("Branch created successfully.")
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
    
}
