package io.modelcontextprotocol.spec;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.type.TypeReference;
import io.modelcontextprotocol.server.McpAsyncServerExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.publisher.Sinks;

/**
 * Represents a Model Control Protocol (MCP) session on the server side. It manages
 * bidirectional JSON-RPC communication with the client.
 */
//代表服务器端上的模型控制协议（MCP）会话。它管理与客户的双向JSON-RPC通信。
public class McpServerSession implements McpSession {

	private static final Logger logger = LoggerFactory.getLogger(McpServerSession.class);

	private final ConcurrentHashMap<Object, MonoSink<McpSchema.JSONRPCResponse>> pendingResponses = new ConcurrentHashMap<>();

	private final String id;

	/** Duration to wait for request responses before timing out */
	private final Duration requestTimeout;

	private final AtomicLong requestCounter = new AtomicLong(0);

	private final InitRequestHandler initRequestHandler;

	private final InitNotificationHandler initNotificationHandler;

	private final Map<String, RequestHandler<?>> requestHandlers;

	private final Map<String, NotificationHandler> notificationHandlers;

	private final McpServerTransport transport;

	private final Sinks.One<McpAsyncServerExchange> exchangeSink = Sinks.one();

	private final AtomicReference<McpSchema.ClientCapabilities> clientCapabilities = new AtomicReference<>();

	private final AtomicReference<McpSchema.Implementation> clientInfo = new AtomicReference<>();

	private static final int STATE_UNINITIALIZED = 0;

	private static final int STATE_INITIALIZING = 1;

	private static final int STATE_INITIALIZED = 2;

	private final AtomicInteger state = new AtomicInteger(STATE_UNINITIALIZED);

	/**
	 * Creates a new server session with the given parameters and the transport to use.
	 * @param id session id
	 * @param transport the transport to use
	 * @param initHandler called when a
	 * {@link io.modelcontextprotocol.spec.McpSchema.InitializeRequest} is received by the
	 * server
	 * @param initNotificationHandler called when a
	 * {@link io.modelcontextprotocol.spec.McpSchema#METHOD_NOTIFICATION_INITIALIZED} is
	 * received.
	 * @param requestHandlers map of request handlers to use
	 * @param notificationHandlers map of notification handlers to use
	 */
	public McpServerSession(String id, Duration requestTimeout, McpServerTransport transport,
			InitRequestHandler initHandler, InitNotificationHandler initNotificationHandler,
			Map<String, RequestHandler<?>> requestHandlers, Map<String, NotificationHandler> notificationHandlers) {
		this.id = id;
		this.requestTimeout = requestTimeout;
		this.transport = transport;
		this.initRequestHandler = initHandler;
		this.initNotificationHandler = initNotificationHandler;
		this.requestHandlers = requestHandlers;
		this.notificationHandlers = notificationHandlers;
	}

	/**
	 * Retrieve the session id.
	 * @return session id
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * Called upon successful initialization sequence between the client and the server
	 * with the client capabilities and information.
	 *
	 * <a href=
	 * "https://github.com/modelcontextprotocol/specification/blob/main/docs/specification/basic/lifecycle.md#initialization">Initialization
	 * Spec</a>
	 * @param clientCapabilities the capabilities the connected client provides
	 * @param clientInfo the information about the connected client
	 */
	public void init(McpSchema.ClientCapabilities clientCapabilities, McpSchema.Implementation clientInfo) {
		//lazySet(V newValue) 方法用于设置 AtomicReference 的值，但不同于普通的 set 方法，它允许 JVM 在某些情况下以更高效的方式更新引用。这种效率的提升主要体现在以下几个方面：
		//	延迟更新:
		//		lazySet 不是立即强制将值更新的，而是允许 JVM 在不影响线程安全性的前提下稍后进行更新操作。这种延迟更新可能在某些架构下会比立即更新更加高效。
		//	避免内存屏障:
		//		lazySet 通常用于减少或者避免不必要的全局内存屏障（memory barriers）。内存屏障是用来保证在多线程环境下内存可见性的，但它们可能会导致性能损耗。
		//		lazySet 的实现可能在某些硬件架构上使用了较弱的内存屏障，因而能提高性能，特别是在写操作频繁但对可见性要求不高的场景中。
		//	最终一致性:
		//		尽管 lazySet 是弱一致性的，最终会被设置为指定的值。即它保证在未来某个时刻会将值更新为指定值，即使它在更新过程中的可见性不如普通的 set 方法。
		//	使用场景
		//		lazySet 通常用于不需要立即对其他线程可见的场景，或者在需要提升性能并且对写操作的实时性要求不高的地方。例如，用于大型对象引用的更新，或者批量数据处理过程中状态标志的更新。
		this.clientCapabilities.lazySet(clientCapabilities);
		this.clientInfo.lazySet(clientInfo);
	}

	private String generateRequestId() {
		return this.id + "-" + this.requestCounter.getAndIncrement();
	}

	@Override
	public <T> Mono<T> sendRequest(String method, Object requestParams, TypeReference<T> typeRef) {
		String requestId = this.generateRequestId();

		// Mono.create 的作用
		//	创建 Mono:
		//		Mono.create 用于创建一个 Mono 实例，允许你使用 sink 来发射（emit）数据、错误或完成信号。
		//		sink 是 MonoSink 的一个实例，提供了 sink.success(value), sink.error(error) 和 sink.complete() 等方法用于管理 Mono 的生命周期。
		//	sink 的来源:
		//		sink 是由 Mono.create 提供的，在这里用于将发送的请求与响应处理逻辑关联起来。它在异步操作中扮演着重要角色。
		return Mono.<McpSchema.JSONRPCResponse>create(sink -> {
			// 挂起响应处理: 将 sink 存储起来，并与请求 ID 关联。
			// 这允许在稍后接收到响应时，能够通过请求 ID 找到对应的 sink 来发射结果或处理错误。
			this.pendingResponses.put(requestId, sink);
			McpSchema.JSONRPCRequest jsonrpcRequest = new McpSchema.JSONRPCRequest(McpSchema.JSONRPC_VERSION, method,
					requestId, requestParams);

			// 负责实际发送请求消息。它返回一个 Mono，可以用于订阅响应处理。
			this.transport.sendMessage(jsonrpcRequest).subscribe(v -> {
			}, error -> {
				// 对发送操作结果进行订阅，处理可能出现的错误。
				// 在这里，如果发送过程中出现错误，响应处理被移除并通过 sink.error(error) 发射错误信号。
				this.pendingResponses.remove(requestId);
				sink.error(error);
			});
			//timeout方法设置一个时限，并在指定时间内等待流发射事件（数据、完成或错误）。如果没有事件发射发生，它将导致流发射一个超时错误
			// .timeout(requestTimeout) 是用来限制从 Mono.create 到执行 handle 的时间。如果在指定的 requestTimeout 时间内没有得到任何 McpSchema.JSONRPCResponse，则会发射一个超时错误信号。这对于处理异步请求非常重要，因为它可以防止请求无限期挂起并确保系统在合理时间内反馈结果。
			// 在 Mono.create 内部，虽然 sink 被关联到请求 ID等待响应，但 timeout 操作符确保无论请求是否正常处理，它都会在限定时间内完成。如果超过时间没有触发任何事件（包括错误），它会自动发出超时错误。
		}).timeout(requestTimeout)
			.doOnError(TimeoutException.class, e -> {
				// Handle timeout error
				System.out.println("Request timed out: " + e.getMessage());
			})
		  //handle 用于对接收到的 JSONRPCResponse 进行自定义处理。
		  //它的 sink 是与 Mono.create 中的相同，代表用于发射结果的通道。
			//handle 的职责: handle 操作符的职责是处理和转换流中的每个元素，它通过 sink.next(value) 发射新的元素，sink.error(error) 发射错误信号，或者 sink.complete() 发射完成信号。
			//错误和完成信号的处理: handle 不处理流的终止信号（错误或完成），这些信号会被传递给流的下游，最终由订阅者处理。
			// handle 操作符无法直接感知到 TimeoutException 或其他的终止信号
		  .handle((jsonRpcResponse, sink) -> {
			// 如果 response 包含错误，则通过 sink.error(new McpError(...)) 发射错误。 技术异步流
			if (jsonRpcResponse.error() != null) {
				sink.error(new McpError(jsonRpcResponse.error()));
			}
			else {
				// 否则，检查 typeRef 是否为 Void，如果是则发射完成信号 sink.complete()，结束异步流
				if (typeRef.getType().equals(Void.class)) {
					sink.complete();
				}
				else {
					sink.next(this.transport.unmarshalFrom(jsonRpcResponse.result(), typeRef));
				}
			}
		});
	}

	@Override
	public Mono<Void> sendNotification(String method, Object params) {
		McpSchema.JSONRPCNotification jsonrpcNotification = new McpSchema.JSONRPCNotification(McpSchema.JSONRPC_VERSION,
				method, params);
		return this.transport.sendMessage(jsonrpcNotification);
	}

	/**
	 * Called by the {@link McpServerTransportProvider} once the session is determined.
	 * The purpose of this method is to dispatch the message to an appropriate handler as
	 * specified by the MCP server implementation
	 * ({@link io.modelcontextprotocol.server.McpAsyncServer} or
	 * {@link io.modelcontextprotocol.server.McpSyncServer}) via
	 * {@link McpServerSession.Factory} that the server creates.
	 * @param message the incoming JSON-RPC message
	 * @return a Mono that completes when the message is processed
	 */
	public Mono<Void> handle(McpSchema.JSONRPCMessage message) {
		return Mono.defer(() -> {
			// TODO handle errors for communication to without initialization happening
			// first
			if (message instanceof McpSchema.JSONRPCResponse response) {
				logger.debug("Received Response: {}", response);
				var sink = pendingResponses.remove(response.id());
				if (sink == null) {
					logger.warn("Unexpected response for unknown id {}", response.id());
				}
				else {
					sink.success(response);
				}
				return Mono.empty();
			}
			else if (message instanceof McpSchema.JSONRPCRequest request) {
				logger.debug("Received request: {}", request);
				return handleIncomingRequest(request).onErrorResume(error -> {
					var errorResponse = new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), null,
							new McpSchema.JSONRPCResponse.JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR,
									error.getMessage(), null));
					// TODO: Should the error go to SSE or back as POST return?
					return this.transport.sendMessage(errorResponse).then(Mono.empty());
				}).flatMap(this.transport::sendMessage);
			}
			else if (message instanceof McpSchema.JSONRPCNotification notification) {
				// TODO handle errors for communication to without initialization
				// happening first
				logger.debug("Received notification: {}", notification);
				// TODO: in case of error, should the POST request be signalled?
				return handleIncomingNotification(notification)
					.doOnError(error -> logger.error("Error handling notification: {}", error.getMessage()));
			}
			else {
				logger.warn("Received unknown message type: {}", message);
				return Mono.empty();
			}
		});
	}

	/**
	 * Handles an incoming JSON-RPC request by routing it to the appropriate handler.
	 * @param request The incoming JSON-RPC request
	 * @return A Mono containing the JSON-RPC response
	 */
	private Mono<McpSchema.JSONRPCResponse> handleIncomingRequest(McpSchema.JSONRPCRequest request) {
		return Mono.defer(() -> {
			Mono<?> resultMono;
			if (McpSchema.METHOD_INITIALIZE.equals(request.method())) {
				// TODO handle situation where already initialized!
				McpSchema.InitializeRequest initializeRequest = transport.unmarshalFrom(request.params(),
						new TypeReference<McpSchema.InitializeRequest>() {
						});

				this.state.lazySet(STATE_INITIALIZING);
				this.init(initializeRequest.capabilities(), initializeRequest.clientInfo());
				resultMono = this.initRequestHandler.handle(initializeRequest);
			}
			else {
				// TODO handle errors for communication to this session without
				// initialization happening first
				var handler = this.requestHandlers.get(request.method());
				if (handler == null) {
					MethodNotFoundError error = getMethodNotFoundError(request.method());
					return Mono.just(new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), null,
							new McpSchema.JSONRPCResponse.JSONRPCError(McpSchema.ErrorCodes.METHOD_NOT_FOUND,
									error.message(), error.data())));
				}

				resultMono = this.exchangeSink.asMono().flatMap(exchange -> handler.handle(exchange, request.params()));
			}
			return resultMono
				.map(result -> new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(), result, null))
				.onErrorResume(error -> Mono.just(new McpSchema.JSONRPCResponse(McpSchema.JSONRPC_VERSION, request.id(),
						null, new McpSchema.JSONRPCResponse.JSONRPCError(McpSchema.ErrorCodes.INTERNAL_ERROR,
								error.getMessage(), null)))); // TODO: add error message
																// through the data field
		});
	}

	/**
	 * Handles an incoming JSON-RPC notification by routing it to the appropriate handler.
	 * @param notification The incoming JSON-RPC notification
	 * @return A Mono that completes when the notification is processed
	 */
	private Mono<Void> handleIncomingNotification(McpSchema.JSONRPCNotification notification) {
		return Mono.defer(() -> {
			if (McpSchema.METHOD_NOTIFICATION_INITIALIZED.equals(notification.method())) {
				this.state.lazySet(STATE_INITIALIZED);
				exchangeSink.tryEmitValue(new McpAsyncServerExchange(this, clientCapabilities.get(), clientInfo.get()));
				return this.initNotificationHandler.handle();
			}

			var handler = notificationHandlers.get(notification.method());
			if (handler == null) {
				logger.error("No handler registered for notification method: {}", notification.method());
				return Mono.empty();
			}
			return this.exchangeSink.asMono().flatMap(exchange -> handler.handle(exchange, notification.params()));
		});
	}

	record MethodNotFoundError(String method, String message, Object data) {
	}

	private MethodNotFoundError getMethodNotFoundError(String method) {
		return new MethodNotFoundError(method, "Method not found: " + method, null);
	}

	@Override
	public Mono<Void> closeGracefully() {
		return this.transport.closeGracefully();
	}

	@Override
	public void close() {
		this.transport.close();
	}

	/**
	 * Request handler for the initialization request.
	 */
	public interface InitRequestHandler {

		/**
		 * Handles the initialization request.
		 * @param initializeRequest the initialization request by the client
		 * @return a Mono that will emit the result of the initialization
		 */
		Mono<McpSchema.InitializeResult> handle(McpSchema.InitializeRequest initializeRequest);

	}

	/**
	 * Notification handler for the initialization notification from the client.
	 */
	public interface InitNotificationHandler {

		/**
		 * Specifies an action to take upon successful initialization.
		 * @return a Mono that will complete when the initialization is acted upon.
		 */
		Mono<Void> handle();

	}

	/**
	 * A handler for client-initiated notifications.
	 */
	public interface NotificationHandler {

		/**
		 * Handles a notification from the client.
		 * @param exchange the exchange associated with the client that allows calling
		 * back to the connected client or inspecting its capabilities.
		 * @param params the parameters of the notification.
		 * @return a Mono that completes once the notification is handled.
		 */
		Mono<Void> handle(McpAsyncServerExchange exchange, Object params);

	}

	/**
	 * A handler for client-initiated requests.
	 *
	 * @param <T> the type of the response that is expected as a result of handling the
	 * request.
	 */
	public interface RequestHandler<T> {

		/**
		 * Handles a request from the client.
		 * @param exchange the exchange associated with the client that allows calling
		 * back to the connected client or inspecting its capabilities.
		 * @param params the parameters of the request.
		 * @return a Mono that will emit the response to the request.
		 */
		Mono<T> handle(McpAsyncServerExchange exchange, Object params);

	}

	/**
	 * Factory for creating server sessions which delegate to a provided 1:1 transport
	 * with a connected client.
	 */
	@FunctionalInterface
	public interface Factory {

		/**
		 * Creates a new 1:1 representation of the client-server interaction.
		 * @param sessionTransport the transport to use for communication with the client.
		 * @return a new server session.
		 */
		McpServerSession create(McpServerTransport sessionTransport);

	}

}
