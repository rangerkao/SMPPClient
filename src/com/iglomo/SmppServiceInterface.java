package com.iglomo;


import java.rmi.RemoteException;

public interface SmppServiceInterface extends java.rmi.Remote{
	public String sendSMPP(String hellostring)throws java.rmi.RemoteException;
}
