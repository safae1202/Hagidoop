package config;

public class Project {
	public static String PATH = "/home/sbelahra/Bureau/PDRn/hagidoop";
	public static String CLIENT_SOCKET_PATH = "147.127.133.69";
	public static int CLIENT_SOCKET_PORT = 4950;
	public static int[] SERVER_NODES_PORTS = {1099, 1088};
	public static String[] SERVER_NODES_PATHS = {"localhost","147.127.133.74"};	
	public static String[] SERVER_NODES_RMI_PATHS = {"rmi://localhost:4000/node1", "rmi://147.127.133.74:4000/node2"};
}