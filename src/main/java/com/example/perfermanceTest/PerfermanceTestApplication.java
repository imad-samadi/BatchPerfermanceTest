package com.example.perfermanceTest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties(BatchProperties.class)
@EnableScheduling
public class PerfermanceTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(PerfermanceTestApplication.class, args);
	}

}
