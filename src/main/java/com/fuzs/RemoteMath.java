package com.fuzs;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteMath extends UnicastRemoteObject implements IRemoteMath {

    private int numberOfComputations;

    protected RemoteMath() throws RemoteException {
        numberOfComputations = 0;
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public double add(double a, double b) throws Exception {
        numberOfComputations++;
        System.out.println("Number of computation perforemd so far = "+ numberOfComputations);

        return a + b;
    }

    @Override
    public double subtract(double a, double b) throws Exception {
        numberOfComputations++;
        System.out.println("Number of computation perforemd so far = "+ numberOfComputations);

        return a - b;
    }
    
}
