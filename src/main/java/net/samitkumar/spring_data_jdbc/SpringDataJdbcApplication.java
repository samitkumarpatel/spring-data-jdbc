package net.samitkumar.spring_data_jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.ListCrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@SpringBootApplication
public class SpringDataJdbcApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringDataJdbcApplication.class, args);
	}

	@Bean
	JsonPlaceholderClient jsonPlaceholderClient(WebClient.Builder webClientBuilder, @Value("${spring.application.jsonplaceholder.url}") String baseUrl) {
		WebClientAdapter adapter = WebClientAdapter.create(webClientBuilder.baseUrl(baseUrl).build());
		HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(adapter).build();
		return factory.createClient(JsonPlaceholderClient.class);
	}

	@Bean
	JdbcCustomConversions jdbcCustomConversions(
			StringToUserReadingConverter stringToUserReadingConverter,
			UserToStringWritingConverter userToStringWritingConverter) {
		return new JdbcCustomConversions(
				java.util.List.of(stringToUserReadingConverter, userToStringWritingConverter));
	}

	@Bean
	RouterFunction<ServerResponse> routerFunction(UserService userService) {
		return RouterFunctions
				.route()
				.GET("/user/{id}", request -> {
					var id = request.pathVariable("id");
					return userService
							.getUserById(id)
							.doOnError(e -> System.out.println(e.getMessage()))
							.onErrorResume(Mono::error)
							.flatMap(ServerResponse.ok()::bodyValue);
				})
				.build();
	}

}

record Address(String street, String suite, String city, String zipcode, Geo geo) { }
record Company(String name, String catchPhrase, String bs) { }
record Geo(Double lat, Double lng) { }
record User(String id, String name, String username, String email, Address address, String phone, String website, Company company) {}

@HttpExchange
interface JsonPlaceholderClient {
	@GetExchange("/users/{id}")
	Mono<User> getUserById(@PathVariable String id);
}

@Service
@RequiredArgsConstructor
class UserService {
	final JsonPlaceholderClient jsonPlaceholderClient;
	final UserAuditRepository userAuditRepository;
	final ObjectMapper objectMapper;

	public Mono<User> getUserById(String id) {
		return jsonPlaceholderClient
				.getUserById(id)
				.flatMap(user -> {
                    try {
                        var data = objectMapper.writeValueAsString(user);
						var userAudit = new UserAudit(null, Integer.valueOf(id), user);
						return Mono.fromCallable(() -> userAuditRepository.save(userAudit))
								.subscribeOn(Schedulers.boundedElastic())
								.thenReturn(user);
                    } catch (JsonProcessingException e) {
                        return Mono.error(new RuntimeException(e));
                    }
				});
	}
}

@Table(name = "users_audit")
record UserAudit(@Id Integer id, Integer userId, User data){}
interface UserAuditRepository extends ListCrudRepository<UserAudit, Integer> {}

@WritingConverter
@Component
class UserToStringWritingConverter implements Converter<User, String> {
	final ObjectMapper objectMapper;

	public UserToStringWritingConverter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}
	@Override
	public String convert(User user) {
		try {
			return objectMapper.writeValueAsString(user);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}

@ReadingConverter
@Component
class StringToUserReadingConverter implements Converter<String, User> {
	final ObjectMapper objectMapper;

	public StringToUserReadingConverter(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public User convert(String source) {
		try {
			return objectMapper.readValue(source, User.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}