package net.samitkumar.spring_data_jdbc;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class SpringDataJdbcApplicationTests {

	@Test
	void contextLoads() {
	}

}
