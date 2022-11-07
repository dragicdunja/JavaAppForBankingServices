/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.banksubsystem1.resources;

import Consts.*;
import entities.Branch;
import entities.Client;
import entities.Location;
import java.util.ArrayList;
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
import resources.Bank1DatabaseData;

/**
 *
 * @author jp170092d
 */

@MessageDriven(activationConfig = {
    @ActivationConfigProperty(propertyName = "destinationLookup",
            propertyValue = "BankSubsystem1Queue"),
    @ActivationConfigProperty(propertyName = "destinationType",
            propertyValue = "javax.jms.Queue")
})
public class MessageDrivenSubsystem1 implements MessageListener {
    
    @PersistenceContext(name = "bank1PU")
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
                System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message received: " + objMsg.getStringProperty(Consts.ACTION));
                
                switch(objMsg.getStringProperty(Consts.ACTION)) {
                    case Consts.GET_ALL_CLIENTS:
                        getAllClients();
                        break;
                    case Consts.CREATE_NEW_CLIENT:
                        createNewClient(objMsg.getStringProperty(Consts.NAME), objMsg.getStringProperty(Consts.ADDRESS), objMsg.getIntProperty(Consts.ID_LOCATION));
                        break;
                    case Consts.CHANGE_CLIENT_LOCATION:
                        changeClientLocation(objMsg.getIntProperty(Consts.ID_CLIENT), objMsg.getIntProperty(Consts.ID_LOCATION));
                        break;
                        
                    case Consts.GET_ALL_LOCATIONS:
                        getAllLocations();
                        break;
                    case Consts.CREATE_NEW_LOCATION:
                        createNewLocation(objMsg.getStringProperty(Consts.NAME), objMsg.getStringProperty(Consts.AREA_CODE));
                        break;
                        
                    case Consts.GET_ALL_BRANCHES:
                        getAllBranches();
                        break;
                    case Consts.CREATE_NEW_BRANCH:
                        createNewBranch(objMsg.getStringProperty(Consts.ADDRESS), objMsg.getIntProperty(Consts.ID_LOCATION));
                        break;
                        
                    case Consts.FETCH_DATABASE:
                        fetchDatabase();
                        break;
                    case Consts.FETCH_DATABASE_DIFFERENCE:
                        fetchDatabaseDifference();
                        break;
                }
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private void getAllClients() {
        
        List<Client> clients = em.createNamedQuery("Client.findAll", Client.class).getResultList();
        ArrayList<Client> response = new ArrayList<>();
        
        clients.forEach((c) -> {
            response.add(c);
        });
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(response);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    private void createNewClient(String name, String address, int idLocation) {
        
        Location location = em.find(Location.class, idLocation);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(location == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No location with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        Client client = new Client();
        client.setFullName(name);
        client.setAddress(address);
        client.setIdLocation(location);
        em.persist(client);
//        int newClientId = em.createQuery("SELECT c.idClient FROM Client c WHERE c.fullName = :name AND c.address = :address AND c.idLocation.idLocation = :idLocation ORDER BY c.idClient DESC", Client.class).getFirstResult();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
//            objMsg.setIntProperty(Consts.RETURN_VALUE, newClientId);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void changeClientLocation(int idClient, int idLocation) {
       
        Client client = em.find(Client.class, idClient);
        Location location = em.find(Location.class, idLocation);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(client == null) {
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No Client with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            return;
        }
        
        if(location == null) {
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No location with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        client.setIdLocation(location);
        em.persist(client);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void getAllLocations() {
        
        List<Location> locations = em.createNamedQuery("Location.findAll", Location.class).getResultList();
        ArrayList<Location> response = new ArrayList<>();
        
        locations.forEach((l) -> {
            response.add(l);
        });
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(response);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void createNewLocation(String name, String areaCode) {
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        Location location = new Location();
        location.setName(name);
        location.setAreaCode(areaCode);
        em.persist(location);
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void getAllBranches() {
        
        List<Branch> branches = em.createNamedQuery("Branch.findAll", Branch.class).getResultList();
        ArrayList<Branch> response = new ArrayList<>();
        
        branches.forEach((b) -> {
            response.add(b);
        });
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(response);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    private void createNewBranch(String address, int idLocation) {
        
        Location location = em.find(Location.class, idLocation);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        if(location == null) {
            
            try {
                objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
                objMsg.setStringProperty(Consts.RESULT, Consts.FAILURE);
                objMsg.setStringProperty(Consts.MESSAGE, "No location with such ID");
                
                producer.send(cbsQueue, objMsg);
            
                System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
            } catch (JMSException ex) {
                Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
            }
            return;
        }
        
        Branch branch = new Branch();
        branch.setAddress(address);
        branch.setIdLocation(location);
        em.persist(branch);
                
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    private void fetchDatabase() {
        
        List<Location> locations = em.createNamedQuery("Location.findAll", Location.class).getResultList();
        ArrayList<Location> responseLocations = new ArrayList<>();
        List<Client> clients = em.createNamedQuery("Client.findAll", Client.class).getResultList();
        ArrayList<Client> responseClients = new ArrayList<>();
        List<Branch> branches = em.createNamedQuery("Branch.findAll", Branch.class).getResultList();
        ArrayList<Branch> responseBranches = new ArrayList<>();
        
        locations.forEach((l) -> {
            responseLocations.add(l);
        });
        clients.forEach((c) -> {
            responseClients.add(c);
        });
        branches.forEach((b) -> {
            responseBranches.add(b);
        });
        
        Bank1DatabaseData b1dd = new Bank1DatabaseData();
        b1dd.setLocations(responseLocations);
        b1dd.setClients(responseClients);
        b1dd.setBranches(responseBranches);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(b1dd);
            
            producer.send(s3Queue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void fetchDatabaseDifference() {
        List<Location> locations = em.createNamedQuery("Location.findAll", Location.class).getResultList();
        ArrayList<Location> responseLocations = new ArrayList<>();
        List<Client> clients = em.createNamedQuery("Client.findAll", Client.class).getResultList();
        ArrayList<Client> responseClients = new ArrayList<>();
        List<Branch> branches = em.createNamedQuery("Branch.findAll", Branch.class).getResultList();
        ArrayList<Branch> responseBranches = new ArrayList<>();
        
        locations.forEach((l) -> {
            responseLocations.add(l);
        });
        clients.forEach((c) -> {
            responseClients.add(c);
        });
        branches.forEach((b) -> {
            responseBranches.add(b);
        });
        
        Bank1DatabaseData b1dd = new Bank1DatabaseData();
        b1dd.setLocations(responseLocations);
        b1dd.setClients(responseClients);
        b1dd.setBranches(responseBranches);
        
        JMSContext context = cf.createContext();
        JMSProducer producer = context.createProducer();
        
        ObjectMessage objMsg = context.createObjectMessage();
        
        try {
            objMsg.setStringProperty(Consts.SENDER, Consts.BANK_SUBSYSTEM_1);
            objMsg.setStringProperty(Consts.RESULT, Consts.SUCCESS);
            
            objMsg.setObject(b1dd);
            
            producer.send(cbsQueue, objMsg);
            
            System.out.println(Consts.BANK_SUBSYSTEM_1 + ": Message sent successfully");
        } catch (JMSException ex) {
            Logger.getLogger(MessageDrivenSubsystem1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
