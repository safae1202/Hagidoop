package daemon;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;

import config.Project;
import interfaces.MapReduce;
import interfaces.NetworkReaderWriter;
import interfaces.PipeReaderWriter;
import io.FileReaderWriterImpl;
import io.NetworkReaderWriterImpl;
import interfaces.FileReaderWriter;
import interfaces.KV;


public class JobLauncher {

	public static void startJob (MapReduce mr, int format, String fname) {
		try {
            // Récupérer la liste des worker nodes de la configuration
            Worker[] workerNodes = getWorkerNodesFromConfig();
            Thread[] threads = new Thread[Project.SERVER_NODES_PORTS.length];
            NetworkReaderWriterImpl serverNetworkReaderWriter = new NetworkReaderWriterImpl(Project.CLIENT_SOCKET_PORT);
            serverNetworkReaderWriter.openServer();

            for (int i = 0; i < workerNodes.length; i++) {
            	final Worker currentWorker = workerNodes[i];
            	String pathToFile = Project.PATH + "/data/" + fname + "-" + (i+1) + ".txt";
            	final FileReaderWriter fileReaderWriter = new FileReaderWriterImpl(pathToFile, FileReaderWriterImpl.FMT_TXT);
            	final NetworkReaderWriterImpl clientNetworkReaderWriter = new NetworkReaderWriterImpl(Project.CLIENT_SOCKET_PATH, Project.CLIENT_SOCKET_PORT);

                threads[i] = new Thread(() -> {
                	try {
						currentWorker.runMap(mr, fileReaderWriter , clientNetworkReaderWriter);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
                });
                threads[i].start();
            }
           
            // Attendre les threads finissent
            try {
            	for (int i = 0; i < threads.length; i++) {
            		threads[i].join();
            	}
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            
            // Le fichier final qu'on produit 
            FileReaderWriterImpl fileToSaveTo = new FileReaderWriterImpl(Project.PATH + "/data/" + "result.txt", FileReaderWriterImpl.FMT_KV);
        	ArrayList<Thread> threads2 = new ArrayList<Thread>();
        	PipeReaderWriter pipe = new PipeReaderWriter();

            int count = 0;
			while (true) {
				if(count >= Project.SERVER_NODES_PATHS.length) {
					break;
				}
				
				NetworkReaderWriterImpl client = (NetworkReaderWriterImpl) serverNetworkReaderWriter.accept();
				count++;
			    System.out.println("************ Client connected ************");

			    // Start a new thread to handle the client
			    // Thread pour écrire les kv sur le pipe 
			    Thread thread = new Thread(() -> {
			    	KV kv = client.read();
			    	while(kv != null) {
			    		pipe.write(kv);
			    		kv = client.read();
			    	}
			    });
			    thread.start();
			    threads2.add(thread);
			    System.out.println("******************************************");
			}
			
            try {
            	for (Thread thread : threads2) {
            		thread.join();
                }
            	fileToSaveTo.open("write");
            	mr.reduce(pipe, fileToSaveTo);
            	fileToSaveTo.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
    private static Worker[] getWorkerNodesFromConfig() {
    	String[] nodesRmiPaths = Project.SERVER_NODES_RMI_PATHS;
    	Worker[] workers = new Worker[Project.SERVER_NODES_RMI_PATHS.length];
    	
    	for (int i = 0; i < nodesRmiPaths.length; i++) {
    		try {
    			workers[i] = (Worker) Naming.lookup(nodesRmiPaths[i]);
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    	return workers;
   }
}
