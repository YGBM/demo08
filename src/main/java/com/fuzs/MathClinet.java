package com.fuzs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MathClinet {

    public static void main(String[] args) throws Exception {

        Registry registry = LocateRegistry.getRegistry("localhost");
        IRemoteMath remoteMath = (IRemoteMath)registry.lookup("Compute");
        double addResult = remoteMath.add(5.0, 3.0);
        System.out.println("5.0 + 3.0 = " + addResult);
        double subResult = remoteMath.subtract(5.0, 3.0);
        System.out.println("5.0 - 3.0 = " + subResult);			
    }
}
