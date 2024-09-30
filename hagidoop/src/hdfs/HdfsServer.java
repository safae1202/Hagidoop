package hdfs;

import config.Project;
import daemon.WorkerImpl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import interfaces.FileReaderWriter;
import interfaces.KV;
import interfaces.NetworkReaderWriter;
import io.FileReaderWriterImpl;
import io.NetworkReaderWriterImpl;

public class HdfsServer {

	public static final int FMT_TXT = 0;
	public static final int FMT_KV = 1;
    private static final String HDFS_BASE_PATH = "/hdfs/";


	private static void usage() {
		System.out.println("Usage: java HdfsServer <socketPortNumber> <nodeName> <rmiPort>");
	}

    public static void main(String[] args) {
    	if (args.length < 3) {
            usage();
            return;
        }

    	// Recuperer le numero du port
    	final int portNumber = Integer.parseInt(args[0]);
    	
    	// Recuperer le nom du noeud
        String nodeName = args[1];
        
    	// Recuperer port RMI
        final int rmiPortNumber = Integer.parseInt(args[2]);;
    	
        Thread threadSocket = new Thread(() -> {
        	System.out.println("********** Listening To Commands Sockets HDFS ************");
        	NetworkReaderWriterImpl serverReaderWriter = new NetworkReaderWriterImpl(portNumber);
        	NetworkReaderWriterImpl acceptedClientReaderWriter = null;

        	serverReaderWriter.openServer();
            while(true) {
            	acceptedClientReaderWriter = (NetworkReaderWriterImpl) serverReaderWriter.accept();
            	KV kv = acceptedClientReaderWriter.read();
            	checkCommand(kv, acceptedClientReaderWriter);
            }  
        });
        
        
        Thread threadRMI = new Thread(() -> {
        	System.out.println("********** Setting Up RMI **********");
    		setUpRMI(rmiPortNumber, nodeName);
        });   
        
        threadSocket.start();
        threadRMI.start();		
    }
    
    private static void setUpRMI(int port, String node) {
    	// Creation d'un registre ou récupartion s'il existe déja
        Registry registry = null;
        try {
        	registry = LocateRegistry.createRegistry(port);
        } catch(RemoteException e) {
        	try {
				registry = LocateRegistry.getRegistry(port);
			} catch (RemoteException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
        }
		
		try {
			WorkerImpl worker = new WorkerImpl();
			Naming.rebind("rmi://localhost:" + port + "/" + node, worker);
			System.out.println("***** RMI: rmi://localhost:" + port + "/" + node + "******");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
    }
    
    private static void checkCommand(KV kv, NetworkReaderWriter networkReaderWriter) {
    	String command = kv.k;
    	String fileName = kv.v;
    	// Traitement pour cas txt ou kv selon le format du fichier du node initial
    	/*
    	int lastDotIndex = fileName.lastIndexOf(".");
    	String fileExtension = fileName.substring(lastDotIndex + 1);
    	if (fileExtension == "txt") {
            FileReaderWriterImpl fileReaderWriter = new FileReaderWriterImpl(Project.PATH + "/data/" + fileName, FMT_TXT);
    	} else {
            FileReaderWriterImpl fileReaderWriter = new FileReaderWriterImpl(Project.PATH + "/data/" + fileName, FMT_KV);
    	}*/
    	    
        FileReaderWriterImpl fileReaderWriter = new FileReaderWriterImpl(Project.PATH + "/data/" + fileName, FMT_TXT);

        switch (command) {
            case "write":
                fileReaderWriter.open("write");
                writeFile(fileReaderWriter, networkReaderWriter);
                fileReaderWriter.close();
                break;
            case "read":
                fileReaderWriter.open("read");
                readFile(fileReaderWriter, networkReaderWriter);
                fileReaderWriter.close();
                break;
            case "delete":
            	deleteFile(kv.v);
        }
    }
    
    // Lecture à partir des networks et Ecriture sur les filereader (fragments)
    private static void writeFile(FileReaderWriter fileReaderWriter, NetworkReaderWriter networkReaderWriter) {
    	KV kv = networkReaderWriter.read();
    	while(kv != null) {
    		fileReaderWriter.write(kv);
			kv = networkReaderWriter.read();
    	}
        System.out.println("Fragment écrit avec succès : " + fileReaderWriter.getFname());
    }
    
    // Suppression d'un fichier en accédant à son répertoire
    private static void deleteFile(String fname) {
        try {
            // le répertoire où sont stockés les fragments sur le nœud
            String fragmentRepertoire = Project.PATH + "/data/";

            // le chemin complet du fragment sur le nœud
            String filePath = fragmentRepertoire + fname;

            // Création d'un objet File pour le fragment
            File file = new File(filePath);

            // Vérification si le fichier existe avant de le supprimer
            if (file.exists()) {
                // Suppression du fichier (fragment)
                if (file.delete()) {
                    System.out.println("Fragment supprimé avec succès : " + fname);
                } else {
                    System.err.println("Échec de la suppression du fragment : " + fname);
                }
            } else {
                System.err.println("Le fragment n'existe pas : " + fname);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // Lecture des filereader (fragments) et Ecriture sur les networks pour que le client puisse accèder au contenu des fragments par les networks
    private static void readFile(FileReaderWriter fileReaderWriter, NetworkReaderWriter networkReaderWriter) {
    	KV kv = fileReaderWriter.read();
    	while(kv != null) {
    		networkReaderWriter.write(kv);
			kv = fileReaderWriter.read();
    	}
    	networkReaderWriter.closeClient();
        System.out.println("Fragment lu avec succès : " + fileReaderWriter.getFname());
    }
}
