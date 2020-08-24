package com.github.birdsnail.source;

import com.github.birdsnail.pojo.HBJJZInfo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;

/**
 * @author BirdSnail
 * @date 2020/8/6
 */
public class SendMessage extends Thread {

	public static final int port = 9986;

	private static final String[] names = {"张三", "李四", "王五"};
	private static final Random random = new Random();


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
			objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
			while (true) {
				String line = objectMapper.writeValueAsString(create(i++));
				System.out.println(line);
				printWriter.println(line);
				printWriter.flush();
				Thread.sleep(2000L);
			}
		} catch (IOException | InterruptedException ex) {
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
		int index = random.nextInt(names.length);
		return HBJJZInfo.builder()
				.id(num)
				.name(names[index])
				.blDate(LocalDate.now())
				.city("wuhan")
				.num(num)
				.validDateTime(LocalDateTime.now())
				.build();
	}

}
