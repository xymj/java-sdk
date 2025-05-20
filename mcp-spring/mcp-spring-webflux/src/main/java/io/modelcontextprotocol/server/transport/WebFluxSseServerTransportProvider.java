package io.modelcontextprotocol.server.transport;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerSession;
import io.modelcontextprotocol.spec.McpServerTransport;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

/**
 * Server-side implementation of the MCP (Model Context Protocol) HTTP transport using
 * Server-Sent Events (SSE). This implementation provides a bidirectional communication
 * channel between MCP clients and servers using HTTP POST for client-to-server messages
 * and SSE for server-to-client messages.
 *
 * <p>
 * Key features:
 * <ul>
 * <li>Implements the {@link McpServerTransportProvider} interface that allows managing
 * {@link McpServerSession} instances and enabling their communication with the
 * {@link McpServerTransport} abstraction.</li>
 * <li>Uses WebFlux for non-blocking request handling and SSE support</li>
 * <li>Maintains client sessions for reliable message delivery</li>
 * <li>Supports graceful shutdown with session cleanup</li>
 * <li>Thread-safe message broadcasting to multiple clients</li>
 * </ul>
 *
 * <p>
 * The transport sets up two main endpoints:
 * <ul>
 * <li>SSE endpoint (/sse) - For establishing SSE connections with clients</li>
 * <li>Message endpoint (configurable) - For receiving JSON-RPC messages from clients</li>
 * </ul>
 *
 * <p>
 * This implementation is thread-safe and can handle multiple concurrent client
 * connections. It uses {@link ConcurrentHashMap} for session management and Project
 * Reactor's non-blocking APIs for message processing and delivery.
 *
 * @author Christian Tzolov
 * @author Alexandros Pappas
 * @author Dariusz Jędrzejczyk
 * @see McpServerTransport
 * @see ServerSentEvent
 */
public class WebFluxSseServerTransportProvider implements McpServerTransportProvider {

	private static final Logger logger = LoggerFactory.getLogger(WebFluxSseServerTransportProvider.class);

	/**
	 * Event type for JSON-RPC messages sent through the SSE connection.
	 */
	public static final String MESSAGE_EVENT_TYPE = "message";

	/**
	 * Event type for sending the message endpoint URI to clients.
	 */
	public static final String ENDPOINT_EVENT_TYPE = "endpoint";

	/**
	 * Default SSE endpoint path as specified by the MCP transport specification.
	 */
	public static final String DEFAULT_SSE_ENDPOINT = "/sse";

	public static final String DEFAULT_BASE_URL = "";

	private final ObjectMapper objectMapper;

	/**
	 * Base URL for the message endpoint. This is used to construct the full URL for
	 * clients to send their JSON-RPC messages.
	 */
	private final String baseUrl;

	private final String messageEndpoint;

	private final String sseEndpoint;

	private final RouterFunction<?> routerFunction;

	private McpServerSession.Factory sessionFactory;

	/**
	 * Map of active client sessions, keyed by session ID.
	 */
	private final ConcurrentHashMap<String, McpServerSession> sessions = new ConcurrentHashMap<>();

	/**
	 * Flag indicating if the transport is shutting down.
	 */
	private volatile boolean isClosing = false;

	/**
	 * Constructs a new WebFlux SSE server transport provider instance with the default
	 * SSE endpoint.
	 * @param objectMapper The ObjectMapper to use for JSON serialization/deserialization
	 * of MCP messages. Must not be null.
	 * @param messageEndpoint The endpoint URI where clients should send their JSON-RPC
	 * messages. This endpoint will be communicated to clients during SSE connection
	 * setup. Must not be null.
	 * @throws IllegalArgumentException if either parameter is null
	 */
	public WebFluxSseServerTransportProvider(ObjectMapper objectMapper, String messageEndpoint) {
		this(objectMapper, messageEndpoint, DEFAULT_SSE_ENDPOINT);
	}

	/**
	 * Constructs a new WebFlux SSE server transport provider instance.
	 * @param objectMapper The ObjectMapper to use for JSON serialization/deserialization
	 * of MCP messages. Must not be null.
	 * @param messageEndpoint The endpoint URI where clients should send their JSON-RPC
	 * messages. This endpoint will be communicated to clients during SSE connection
	 * setup. Must not be null.
	 * @throws IllegalArgumentException if either parameter is null
	 */
	public WebFluxSseServerTransportProvider(ObjectMapper objectMapper, String messageEndpoint, String sseEndpoint) {
		this(objectMapper, DEFAULT_BASE_URL, messageEndpoint, sseEndpoint);
	}

	/**
	 * Constructs a new WebFlux SSE server transport provider instance.
	 * @param objectMapper The ObjectMapper to use for JSON serialization/deserialization
	 * of MCP messages. Must not be null.
	 * @param baseUrl webflux message base path
	 * @param messageEndpoint The endpoint URI where clients should send their JSON-RPC
	 * messages. This endpoint will be communicated to clients during SSE connection
	 * setup. Must not be null.
	 * @throws IllegalArgumentException if either parameter is null
	 */
	public WebFluxSseServerTransportProvider(ObjectMapper objectMapper, String baseUrl, String messageEndpoint,
			String sseEndpoint) {
		Assert.notNull(objectMapper, "ObjectMapper must not be null");
		Assert.notNull(baseUrl, "Message base path must not be null");
		Assert.notNull(messageEndpoint, "Message endpoint must not be null");
		Assert.notNull(sseEndpoint, "SSE endpoint must not be null");

		this.objectMapper = objectMapper;
		this.baseUrl = baseUrl;
		this.messageEndpoint = messageEndpoint;
		this.sseEndpoint = sseEndpoint;
		this.routerFunction = RouterFunctions.route()
			.GET(this.sseEndpoint, this::handleSseConnection)
			.POST(this.messageEndpoint, this::handleMessage)
			.build();
	}

	@Override
	public void setSessionFactory(McpServerSession.Factory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Broadcasts a JSON-RPC message to all connected clients through their SSE
	 * connections. The message is serialized to JSON and sent as a server-sent event to
	 * each active session.
	 *
	 * <p>
	 * The method:
	 * <ul>
	 * <li>Serializes the message to JSON</li>
	 * <li>Creates a server-sent event with the message data</li>
	 * <li>Attempts to send the event to all active sessions</li>
	 * <li>Tracks and reports any delivery failures</li>
	 * </ul>
	 * @param method The JSON-RPC method to send to clients
	 * @param params The method parameters to send to clients
	 * @return A Mono that completes when the message has been sent to all sessions, or
	 * errors if any session fails to receive the message
	 */
	@Override
	public Mono<Void> notifyClients(String method, Object params) {
		if (sessions.isEmpty()) {
			logger.debug("No active sessions to broadcast message to");
			return Mono.empty();
		}

		logger.debug("Attempting to broadcast message to {} active sessions", sessions.size());

		return Flux.fromIterable(sessions.values())
			.flatMap(session -> session.sendNotification(method, params)
				.doOnError(
						e -> logger.error("Failed to send message to session {}: {}", session.getId(), e.getMessage()))
				.onErrorComplete())
			.then();
	}

	// FIXME: This javadoc makes claims about using isClosing flag but it's not
	// actually
	// doing that.
	/**
	 * Initiates a graceful shutdown of all the sessions. This method ensures all active
	 * sessions are properly closed and cleaned up.
	 *
	 * <p>
	 * The shutdown process:
	 * <ul>
	 * <li>Marks the transport as closing to prevent new connections</li>
	 * <li>Closes each active session</li>
	 * <li>Removes closed sessions from the sessions map</li>
	 * <li>Times out after 5 seconds if shutdown takes too long</li>
	 * </ul>
	 * @return A Mono that completes when all sessions have been closed
	 */
	@Override
	public Mono<Void> closeGracefully() {
		return Flux.fromIterable(sessions.values())
			.doFirst(() -> logger.debug("Initiating graceful shutdown with {} active sessions", sessions.size()))
			.flatMap(McpServerSession::closeGracefully)
			// Flux.then() 是一个 流程控制操作符，用于在 源 Flux 完成（成功或失败）后，执行另一个 Mono 或 Flux 操作。它类似于传统的 "finally" 逻辑，但以响应式的方式处理。
			// 核心作用
			//		在源 Flux 完成时触发后续操作：
			//		不管源 Flux 是成功完成、错误还是被取消，then() 都会执行后续操作。
			//		后续操作的结果会作为新序列的值或完成信号。
			// 关键特性
			//		不依赖源 Flux 的值：then() 的结果与源 Flux 的数据无关，仅在源完成时触发。
			//		支持链式操作：可以返回新的 Mono 或 Flux，继续链式处理。
			//		错误处理：如果源 Flux 发生错误，then() 仍会执行，但错误会被传递到最终的序列。
			// 关键行为说明
			//		仅在源 Flux 完成时触发：包括成功完成、错误或取消。
			//		顺序执行：先处理完源 Flux 的所有项，再执行 then() 的操作。
			//		错误传播：如果源 Flux 发生错误，then() 的结果会携带该错误。
			// 与 doFinally() 的区别
			//		方法	用途	返回类型	是否阻塞
			//		then()	执行后续操作并返回新序列	Mono 或 Flux	否
			//		doFinally()	添加完成时的副作用（如日志）	Flux（原序列）	否

			// 示例详解
			//示例 1：简单 then()（无参数）
			//Flux.just(1, 2, 3)
			//    .doOnNext(i -> System.out.println("Processing: " + i))
			//    .then() // 返回 Mono<Void>
			//    .subscribe(() -> System.out.println("All items processed!"));
			//输出：
			//Processing: 1
			//Processing: 2
			//Processing: 3
			//All items processed!

			//示例 2：带 Mono 参数的 then()
			//Flux<User> users = Flux.just(new User("Alice"), new User("Bob"));
			//
			//Mono<String> result = users
			//    .flatMap(user -> saveUser(user)) // 异步保存用户
			//    .then(Mono.just("Users saved successfully")); // 完成后返回结果
			//
			//result.subscribe(System.out::println); // 输出：Users saved successfully

			//示例 3：带 Flux 参数的 then()
			//Flux<User> users = Flux.just(new User("Alice"), new User("Bob"));
			//Flux<Post> posts = Flux.just(new Post("Post 1"), new Post("Post 2"));
			//
			//Flux<String> combined = users
			//    .flatMap(user -> saveUser(user))
			//    .then(posts
			//        .flatMap(post -> savePost(post))
			//    )
			//    .then(Flux.just("All users and posts saved!"));
			//
			//combined.subscribe(System.out::println);
			//// 输出：All users and posts saved!

			.then();
	}

	/**
	 * Returns the WebFlux router function that defines the transport's HTTP endpoints.
	 * This router function should be integrated into the application's web configuration.
	 *
	 * <p>
	 * The router function defines two endpoints:
	 * <ul>
	 * <li>GET {sseEndpoint} - For establishing SSE connections</li>
	 * <li>POST {messageEndpoint} - For receiving client messages</li>
	 * </ul>
	 * @return The configured {@link RouterFunction} for handling HTTP requests
	 */
	public RouterFunction<?> getRouterFunction() {
		return this.routerFunction;
	}

	/**
	 * Handles new SSE connection requests from clients. Creates a new session for each
	 * connection and sets up the SSE event stream.
	 * @param request The incoming server request
	 * @return A Mono which emits a response with the SSE event stream
	 */
	private Mono<ServerResponse> handleSseConnection(ServerRequest request) {
		if (isClosing) {
			return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).bodyValue("Server is shutting down");
		}

		return ServerResponse.ok()
			.contentType(MediaType.TEXT_EVENT_STREAM)
			.body(Flux.<ServerSentEvent<?>>create(sink -> {
				WebFluxMcpSessionTransport sessionTransport = new WebFluxMcpSessionTransport(sink);

				McpServerSession session = sessionFactory.create(sessionTransport);
				String sessionId = session.getId();

				logger.debug("Created new SSE connection for session: {}", sessionId);
				sessions.put(sessionId, session);

				// Send initial endpoint event
				logger.debug("Sending initial endpoint event to session: {}", sessionId);
				sink.next(ServerSentEvent.builder()
					.event(ENDPOINT_EVENT_TYPE)
					.data(this.baseUrl + this.messageEndpoint + "?sessionId=" + sessionId)
					.build());
				sink.onCancel(() -> {
					logger.debug("Session {} cancelled", sessionId);
					sessions.remove(sessionId);
				});
			}), ServerSentEvent.class);
	}

	/**
	 * Handles incoming JSON-RPC messages from clients. Deserializes the message and
	 * processes it through the configured message handler.
	 *
	 * <p>
	 * The handler:
	 * <ul>
	 * <li>Deserializes the incoming JSON-RPC message</li>
	 * <li>Passes it through the message handler chain</li>
	 * <li>Returns appropriate HTTP responses based on processing results</li>
	 * <li>Handles various error conditions with appropriate error responses</li>
	 * </ul>
	 * @param request The incoming server request containing the JSON-RPC message
	 * @return A Mono emitting the response indicating the message processing result
	 */
	private Mono<ServerResponse> handleMessage(ServerRequest request) {
		if (isClosing) {
			return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE).bodyValue("Server is shutting down");
		}

		if (request.queryParam("sessionId").isEmpty()) {
			return ServerResponse.badRequest().bodyValue(new McpError("Session ID missing in message endpoint"));
		}

		McpServerSession session = sessions.get(request.queryParam("sessionId").get());

		if (session == null) {
			return ServerResponse.status(HttpStatus.NOT_FOUND)
				.bodyValue(new McpError("Session not found: " + request.queryParam("sessionId").get()));
		}
		// Mono map和flatMap区别:
		//		map	同步转换、无异步依赖	Mono<U>	否
		//		flatMap	异步操作、需要处理返回的 Mono	Mono<U>	是
		// 关键原则
		//		map：仅用于 同步转换，返回 非 Mono 类型。类似于 函数式编程中的 map，直接转换值。
		//		flatMap：用于 异步操作 或 处理嵌套 Mono，返回 Mono<U>。类似于 flatMap 在流处理中的作用，展开并合并结果流。
		// 数据流流程对比:
		//		map 的数据流
		//			Mono<T> → map(f: T → U) → Mono<U>
		//			流程：直接转换当前值，不触发异步操作。
		//		flatMap 的数据流
		//			Mono<T> → flatMap(f: T → Mono<U>) → Mono<U>
		//			流程：
		//				调用 f(T) 获取 Mono<U>。
		//				订阅并展开 返回的 Mono<U>，继续链式处理。
		return request.bodyToMono(String.class).flatMap(body -> {
			try {
				McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(objectMapper, body);
				return session.handle(message).flatMap(response -> ServerResponse.ok().build()).onErrorResume(error -> {
					logger.error("Error processing  message: {}", error.getMessage());
					// TODO: instead of signalling the error, just respond with 200 OK
					// - the error is signalled on the SSE connection
					// return ServerResponse.ok().build();
					return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR)
						.bodyValue(new McpError(error.getMessage()));
				});
			}
			catch (IllegalArgumentException | IOException e) {
				logger.error("Failed to deserialize message: {}", e.getMessage());
				return ServerResponse.badRequest().bodyValue(new McpError("Invalid message format"));
			}
		});
	}

	private class WebFluxMcpSessionTransport implements McpServerTransport {

		private final FluxSink<ServerSentEvent<?>> sink;

		public WebFluxMcpSessionTransport(FluxSink<ServerSentEvent<?>> sink) {
			this.sink = sink;
		}

		@Override
		public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {
			return Mono.fromSupplier(() -> {
				try {
					return objectMapper.writeValueAsString(message);
				}
				catch (IOException e) {
					throw Exceptions.propagate(e);
				}
			}).doOnNext(jsonText -> {
				ServerSentEvent<Object> event = ServerSentEvent.builder()
					.event(MESSAGE_EVENT_TYPE)
					.data(jsonText)
					.build();
				sink.next(event);
			}).doOnError(e -> {
				// TODO log with sessionid
				Throwable exception = Exceptions.unwrap(e);
				sink.error(exception);
			}).then();
		}

		@Override
		public <T> T unmarshalFrom(Object data, TypeReference<T> typeRef) {
			return objectMapper.convertValue(data, typeRef);
		}

		@Override
		public Mono<Void> closeGracefully() {
			return Mono.fromRunnable(sink::complete);
		}

		@Override
		public void close() {
			sink.complete();
		}

	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for creating instances of {@link WebFluxSseServerTransportProvider}.
	 * <p>
	 * This builder provides a fluent API for configuring and creating instances of
	 * WebFluxSseServerTransportProvider with custom settings.
	 */
	public static class Builder {

		private ObjectMapper objectMapper;

		private String baseUrl = DEFAULT_BASE_URL;

		private String messageEndpoint;

		private String sseEndpoint = DEFAULT_SSE_ENDPOINT;

		/**
		 * Sets the ObjectMapper to use for JSON serialization/deserialization of MCP
		 * messages.
		 * @param objectMapper The ObjectMapper instance. Must not be null.
		 * @return this builder instance
		 * @throws IllegalArgumentException if objectMapper is null
		 */
		public Builder objectMapper(ObjectMapper objectMapper) {
			Assert.notNull(objectMapper, "ObjectMapper must not be null");
			this.objectMapper = objectMapper;
			return this;
		}

		/**
		 * Sets the project basePath as endpoint prefix where clients should send their
		 * JSON-RPC messages
		 * @param baseUrl the message basePath . Must not be null.
		 * @return this builder instance
		 * @throws IllegalArgumentException if basePath is null
		 */
		public Builder basePath(String baseUrl) {
			Assert.notNull(baseUrl, "basePath must not be null");
			this.baseUrl = baseUrl;
			return this;
		}

		/**
		 * Sets the endpoint URI where clients should send their JSON-RPC messages.
		 * @param messageEndpoint The message endpoint URI. Must not be null.
		 * @return this builder instance
		 * @throws IllegalArgumentException if messageEndpoint is null
		 */
		public Builder messageEndpoint(String messageEndpoint) {
			Assert.notNull(messageEndpoint, "Message endpoint must not be null");
			this.messageEndpoint = messageEndpoint;
			return this;
		}

		/**
		 * Sets the SSE endpoint path.
		 * @param sseEndpoint The SSE endpoint path. Must not be null.
		 * @return this builder instance
		 * @throws IllegalArgumentException if sseEndpoint is null
		 */
		public Builder sseEndpoint(String sseEndpoint) {
			Assert.notNull(sseEndpoint, "SSE endpoint must not be null");
			this.sseEndpoint = sseEndpoint;
			return this;
		}

		/**
		 * Builds a new instance of {@link WebFluxSseServerTransportProvider} with the
		 * configured settings.
		 * @return A new WebFluxSseServerTransportProvider instance
		 * @throws IllegalStateException if required parameters are not set
		 */
		public WebFluxSseServerTransportProvider build() {
			Assert.notNull(objectMapper, "ObjectMapper must be set");
			Assert.notNull(messageEndpoint, "Message endpoint must be set");

			return new WebFluxSseServerTransportProvider(objectMapper, baseUrl, messageEndpoint, sseEndpoint);
		}

	}

}
