package net.samitkumar.spring_data_jdbc;

import org.springframework.boot.SpringApplication;

public class TestSpringDataJdbcApplication {

	public static void main(String[] args) {
		SpringApplication.from(SpringDataJdbcApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
