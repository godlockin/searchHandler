package com.searchhandler;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = {"com.searchhandler.mapper"})
public class SearchHandlerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SearchHandlerApplication.class, args);
	}
}
