package com.fuzs;

import java.rmi.Remote;

public interface IRemoteMath extends Remote {

    public double add(double a,double b)throws Exception;
    public double subtract(double a,double b)throws Exception;

}
