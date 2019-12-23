package org.tugraz.sysds.pythonapi;

import org.tugraz.sysds.api.jmlc.Connection;
import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class pythonDMLScript {
	private Connection _connection;
	
	public static void main(String[] args) {
		GatewayServer gatewayServer = new GatewayServer(new pythonDMLScript());
		gatewayServer.start();
		System.out.println("Gateway Server Started");
		// copied from: https://stackoverflow.com/questions/43282447/py4j-launch-gateway-not-connecting-properly
		if (args.length == 1 && args[0].equals("--die-on-exit")) {
			try {
				BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
				stdin.readLine();
				System.exit(0);
			}
			catch (java.io.IOException e) {
				System.exit(1);
			}
		}
	}
	
	public pythonDMLScript() {
		_connection = new Connection();
	}
	
	public Connection getConnection() {
		return _connection;
	}
}
