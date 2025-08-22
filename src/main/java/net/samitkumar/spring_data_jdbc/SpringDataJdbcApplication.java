package net.samitkumar.spring_data_jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.convert.JdbcCustomConversions;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.ListCrudRepository;
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
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
	JdbcCustomConversions jdbcCustomConversions() {
		return new JdbcCustomConversions(
				List.of(
						new UserToStringConverter(),
						new PGobjectToUserConverter()
                )
		);
	}

	@Bean
	RouterFunction<ServerResponse> routerFunction(UserService userService) {
		return RouterFunctions
				.route()
				.path("/db", builder -> builder
						.GET("", request -> userService.getAllDbUsers().flatMap(ServerResponse.ok()::bodyValue))
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
	final ObjectMapper objectMapper;

	public Mono<User> getUserById(String id) {
		return Mono.fromCallable(() -> userAuditRepository
				.findById(Integer.valueOf(id)).orElse(new UserAudit(null,null,null)))
				.subscribeOn(Schedulers.boundedElastic())
				.doOnSuccess(u -> log.info("DB user value {}", u))
				.mapNotNull(UserAudit::data)
				.switchIfEmpty(jsonPlaceholderClient.getUserById(id)
						.mapNotNull(u -> new UserAudit(null, Integer.valueOf(id), u))
						.doOnSuccess(u -> log.info("WEB user value {}", u))
						.onErrorResume(e -> Mono.error(new UserNotFoundException(e.getMessage())))
						.mapNotNull(userAuditRepository::save)
						.map(UserAudit::data));
	}

	public Mono<List<UserAudit>> getAllDbUsers() {
		return Mono.fromCallable(userAuditRepository::findAll)
				.subscribeOn(Schedulers.boundedElastic());
	}

	public Mono<UserAudit> getDbUserById(String id) {
		return Mono.fromCallable(() -> userAuditRepository.findById(Integer.valueOf(id)).orElseThrow())
				.subscribeOn(Schedulers.boundedElastic());
	}
}

@Table(name = "users_audit")
record UserAudit(@Id Integer id, Integer userId, User data){}
interface UserAuditRepository extends ListCrudRepository<UserAudit, Integer> {}

class UserToStringConverter implements Converter<User, String> {
	@Override
	public String convert(@Nonnull User user) {
		try {
			return new ObjectMapper().writeValueAsString(user);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
}

class PGobjectToUserConverter implements Converter<PGobject, User> {
	@Override
	public User convert(PGobject source) {
		try {
			return new ObjectMapper().readValue(source.getValue(), User.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}
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