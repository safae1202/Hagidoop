package io;

import java.net.*;
import java.io.*;

import interfaces.KV;
import interfaces.NetworkReaderWriter;

public class NetworkReaderWriterImpl implements NetworkReaderWriter{
	
	private Socket clientSocket;
    private ServerSocket serverSocket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private String serverAddress;
    private int serverPort;
    
    // Client avec Socket
    public NetworkReaderWriterImpl(Socket socket) {
        try {
            clientSocket = socket;
			reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	        writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    // Constructeur pour le côté client
    public NetworkReaderWriterImpl(String serverAddress, int serverPort) {
    	this.serverPort = serverPort;
    	this.serverAddress = serverAddress;
    }

    // Constructeur pour le côté serveur
    public NetworkReaderWriterImpl(int serverPort) {
    	this.serverPort = serverPort;
    }

	@Override
	public KV read() {
		try {
            String line = reader.readLine();
            return parseKV(line);
			            
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return null;
	}

	@Override
	public void write(KV record) {
		 try {
	            writer.write(formatKV(record));
	            writer.newLine();
	            writer.flush(); // pour vider le tampon
				
	        } catch (IOException e) {
	            e.printStackTrace();
	        }		
	}

	@Override
	public void openServer() {
		 try {
			serverSocket = new ServerSocket(serverPort);
		 } catch (IOException e) {
			 e.printStackTrace();
	     }		
	}

	@Override
	public void openClient() {
		try {
			clientSocket = new Socket(serverAddress, serverPort);
			reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            System.out.println("Connexion établie avec le serveur.");
        } catch (IOException e) {
            e.printStackTrace();
        }
		
	}
	
	
	@Override
	public NetworkReaderWriter accept() {
		try {
			NetworkReaderWriter client  = new NetworkReaderWriterImpl(serverSocket.accept());
			return client;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
	}
	

	@Override
	public void closeServer() {
		try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }		
	}

	@Override
	public void closeClient() {
		try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }		
	}
	
	private KV parseKV(String line) {
        if (line != null) {
        	String[] parts = line.split(KV.SEPARATOR);
            String key = parts[0].trim();
            String value = parts[1].trim();
            return new KV(key,value);
        }
        return null;
    }

    private String formatKV(KV record) {
    	return record.k + KV.SEPARATOR + record.v;
    }
	

}
