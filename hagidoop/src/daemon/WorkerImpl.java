package daemon;

import interfaces.FileReaderWriter;

//package hagidoop;

import interfaces.Map;
import interfaces.NetworkReaderWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import config.Project;

public class WorkerImpl extends UnicastRemoteObject implements Worker {

    public WorkerImpl() throws RemoteException {
        // Constructeur
    }

	@Override
	public void runMap(Map m, FileReaderWriter reader, NetworkReaderWriter writer) throws RemoteException {
		
		System.out.println("***************** Working running map ********************");

		// Initialiser le serveur RMI
		
		// Initialiser le reader
		
		// Initialiser le writer
		
		// Appel a la fonction map
		writer.openClient();
		reader.open("read");
		m.map(reader, writer);
        System.out.println("***************************************************");
        reader.close();
        writer.closeClient();
	}
	

}
