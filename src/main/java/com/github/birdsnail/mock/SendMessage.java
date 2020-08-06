package com.github.birdsnail.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.birdsnail.pojo.HBJJZInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDate;

/**
 * @author BirdSnail
 * @date 2020/8/6
 */
public class SendMessage extends Thread {

	public static final int port = 9986;


	@Override
	public void run() {
		PrintWriter printWriter = null;
		try {
			ServerSocket serverSocket = new ServerSocket(port);
			Socket socket = serverSocket.accept();
			System.out.println("came a request");
			printWriter = new PrintWriter(socket.getOutputStream());
			int i = 0;
			ObjectMapper objectMapper = new ObjectMapper();
			while (true) {
				String line = objectMapper.writeValueAsString(create(i++));
				System.out.println(line);
				printWriter.println(line);
				printWriter.flush();
				Thread.sleep(2000L);
			}
		} catch (IOException | InterruptedException ex) {
			printWriter.flush();
			printWriter.close();
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) throws InterruptedException {
		SendMessage sendMessage = new SendMessage();
		sendMessage.start();
		sendMessage.join();
	}

	private HBJJZInfo create(int num) {
		return HBJJZInfo.builder()
				.name("nihao")
				.blDate(LocalDate.now())
				.city("wuhan")
				.num(num)
				.validDate(LocalDate.now())
				.build();
	}
}
