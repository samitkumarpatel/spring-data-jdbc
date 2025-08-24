package net.samitkumar.spring_data_jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.ConnectionFactory;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.support.WebClientAdapter;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

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
	RouterFunction<ServerResponse> routerFunction(UserService userService) {
		return RouterFunctions
				.route()
				.path("/db", builder -> builder
						.GET("", request -> userService.getAllDbUsers().collectList().flatMap(ServerResponse.ok()::bodyValue))
						.GET("/{id}", request -> userService.getDbUserById(request.pathVariable("id")).flatMap(ServerResponse.ok()::bodyValue))
						.build()
				)
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
@Slf4j
class UserService {
	final JsonPlaceholderClient jsonPlaceholderClient;
	final UserAuditRepository userAuditRepository;

	public Mono<User> getUserById(String id) {
		return userAuditRepository
				.findByUserId(Integer.valueOf(id)).mapNotNull(UserAudit::data)
				.doOnSuccess(u -> log.info("DB getUserById {}", u))
				.switchIfEmpty(jsonPlaceholderClient
						.getUserById(id)
						.doOnSuccess(u -> log.info("WEB getUserById {}", u))
						.onErrorResume(e -> Mono.error(new UserNotFoundException(e.getMessage())))
						.map(u -> new UserAudit(null, Integer.valueOf(u.id()), u))
						.flatMap(userAuditRepository::save)
						.map(UserAudit::data));
	}

	public Flux<UserAudit> getAllDbUsers() {
		return userAuditRepository.findAll();
	}

	public Mono<UserAudit> getDbUserById(String id) {
		return userAuditRepository
				.findById(Integer.valueOf(id));
	}
}

@Table(name = "users_audit")
record UserAudit(@Id Integer id, Integer userId, User data){}

interface UserAuditRepository extends R2dbcRepository<UserAudit, Integer> {
	Mono<UserAudit> findByUserId(Integer userId);
}

@ResponseStatus(HttpStatus.NOT_FOUND)
class UserNotFoundException extends RuntimeException {

	public UserNotFoundException() {
		super("User not found");
	}

	public UserNotFoundException(String message) {
		super(message);
	}
}

class UserToJsonConvertor implements Converter<User, Json>	{
	@Override
	public Json convert(@Nonnull User user) {
		try {
			return Json.of(new ObjectMapper().writeValueAsString(user));
		} catch (Exception e) {
			throw new RuntimeException("Error converting User to JSON", e);
		}
	}
}

class JsonToUserConvertor implements Converter<Json, User> {
	@Override
	public User convert(@Nonnull Json json) {
		try {
			return new ObjectMapper().readValue(json.asString(), User.class);
		} catch (Exception e) {
			throw new RuntimeException("Error converting JSON to User", e);
		}
	}
}

@Configuration
@RequiredArgsConstructor
class DbConfig extends AbstractR2dbcConfiguration {
	final ConnectionFactory connectionFactory;

	@Override
	@Nonnull
	public ConnectionFactory connectionFactory() {
		return connectionFactory;
	}

	@Override
	@Nonnull
	protected List<Object> getCustomConverters() {
		return List.of(
				new UserToJsonConvertor(),
				new JsonToUserConvertor()
		);
	}
}