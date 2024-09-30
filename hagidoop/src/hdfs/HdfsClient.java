package hdfs;

import java.util.ArrayList;

import config.Project;
import interfaces.FileReaderWriter;
import interfaces.HdfsKV;
import interfaces.NetworkReaderWriter;
import io.FileReaderWriterImpl;
import io.NetworkReaderWriterImpl;
import interfaces.KV;

public class HdfsClient {

    private static final String HDFS_URI = "hdfs://your-hdfs-server:port"; // Remplacez par l'URI de votre serveur HDFS

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <txt|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	public static void HdfsDelete(String fname) {
		NetworkReaderWriter[] serverReaderWriters = getServerReaderWriters().toArray(new NetworkReaderWriter[Project.SERVER_NODES_PORTS.length]);

		// Envoyer HDFS KV pour la configuration
        for (int i = 0; i < serverReaderWriters.length; i++) {
        	serverReaderWriters[i].write(new HdfsKV("delete", fname + "-" + (i+1) + ".txt"));
        }
	}

	public static void HdfsWrite(int fmt, String fname) {
		// Traitement d'un fichier txt ou kv à l'entrée
		/*FileReaderWriterImpl fichierSourceADecoupterReaderWriter = null;
		
		if (fmt == 0) {
		     fichierSourceADecoupterReaderWriter = new FileReaderWriterImpl(Project.PATH + "/data/" + fname + ".txt", fmt);
		} else {
			 fichierSourceADecoupterReaderWriter = new FileReaderWriterImpl(Project.PATH + "/data/" + fname + ".kv", fmt);
		}
		*/
		// Lire le fichier local 
		FileReaderWriterImpl fichierSourceADecoupterReaderWriter = new FileReaderWriterImpl(Project.PATH + "/data/" + fname + ".txt", fmt);
		fichierSourceADecoupterReaderWriter.open("read");

		// Recuperer la liste des noeuds
		NetworkReaderWriter[] serverReaderWriters = getServerReaderWriters().toArray(new NetworkReaderWriter[Project.SERVER_NODES_PORTS.length]);
		
		// Envoyer HDFS KV pour la configuration
        for (int i = 0; i < serverReaderWriters.length; i++) {        	
        	serverReaderWriters[i].write(new HdfsKV("write", fname + "-" + (i+1) + ".txt"));
        }
        
        // Decouper le fichier sur les differents reader writers
     	KV kv = fichierSourceADecoupterReaderWriter.read();
     	int readerWriterIndex = 0;
        while(kv != null) {
			NetworkReaderWriter currentReaderWriter = serverReaderWriters[readerWriterIndex];
			currentReaderWriter.write(kv);
			kv = fichierSourceADecoupterReaderWriter.read();
			readerWriterIndex = (readerWriterIndex + 1) % serverReaderWriters.length;
        }
        
        // Fermer Server Sockets
        for (NetworkReaderWriter serverReaderWriter : serverReaderWriters) {
        	serverReaderWriter.closeServer();
        }

        // Fermer File Reader
		fichierSourceADecoupterReaderWriter.close();
	}


	public static void HdfsRead(String fname) {
		// Lire le fichier local
		FileReaderWriterImpl fichierACreer = new FileReaderWriterImpl(Project.PATH + "/data/" + fname + ".txt", FileReaderWriter.FMT_TXT);
		fichierACreer.open("write");

		// Recuperer la liste des noeuds
		NetworkReaderWriter[] serverReaderWriters = getServerReaderWriters().toArray(new NetworkReaderWriter[Project.SERVER_NODES_PORTS.length]);
		
		// Envoyer HDFS KV pour la configuration
        for (int i = 0; i < serverReaderWriters.length; i++) {        	
        	serverReaderWriters[i].write(new HdfsKV("read", fname + "-" + (i+1) + ".txt"));
        }
        
        // Lecture au niveau des networks et Ecriture au niveau du fichier résultant
		for (NetworkReaderWriter currentReaderWriter : serverReaderWriters) {
			KV kv = currentReaderWriter.read();
	        while(kv != null) {
	        	fichierACreer.write(kv);
	        	kv = currentReaderWriter.read();
	        }		
		}

		fichierACreer.close();
	}
	

	public static void main(String[] args) {
		// java HdfsClient <read|write> <txt|kv> <file>
		// appel des méthodes précédentes depuis la ligne de commande

		if (args.length < 2) {
            usage();
            return;
        }

		String command = args[0];

        switch (command) {
            case "read":
                HdfsRead(args[1]);
                break;
            case "write":
                String fileFormat = args[1];
                int format = fileFormat.equals("txt") ? FileReaderWriter.FMT_TXT : FileReaderWriter.FMT_KV;
                HdfsWrite(format, args[2]);
                break;
            case "delete":
                HdfsDelete(args[1]);
                break;
            default:
                usage();
                break;
        }
	}

	// Retourne la liste des networks (sockets) associés aux nodes
    private static ArrayList<NetworkReaderWriterImpl> getServerReaderWriters() {
    	int[] serverSocketPorts = Project.SERVER_NODES_PORTS;
    	String[] serverSocketPaths = Project.SERVER_NODES_PATHS;
    	ArrayList<NetworkReaderWriterImpl> serverReaderWriters = new ArrayList<NetworkReaderWriterImpl>();

    	for (int i = 0; i < serverSocketPorts.length; i++) {
    		NetworkReaderWriterImpl client = new NetworkReaderWriterImpl(serverSocketPaths[i], serverSocketPorts[i]);
    		client.openClient();
    		serverReaderWriters.add(client);
        }
    	return serverReaderWriters;
    }
}
