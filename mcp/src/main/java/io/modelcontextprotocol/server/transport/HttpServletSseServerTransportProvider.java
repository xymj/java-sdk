/*
 * Copyright 2024 - 2024 the original author or authors.
 */
package io.modelcontextprotocol.server.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerSession;
import io.modelcontextprotocol.spec.McpServerTransport;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import io.modelcontextprotocol.util.Assert;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A Servlet-based implementation of the MCP HTTP with Server-Sent Events (SSE) transport
 * specification. This implementation provides similar functionality to
 * WebFluxSseServerTransportProvider but uses the traditional Servlet API instead of
 * WebFlux.
 *
 * <p>
 * The transport handles two types of endpoints:
 * <ul>
 * <li>SSE endpoint (/sse) - Establishes a long-lived connection for server-to-client
 * events</li>
 * <li>Message endpoint (configurable) - Handles client-to-server message requests</li>
 * </ul>
 *
 * <p>
 * Features:
 * <ul>
 * <li>Asynchronous message handling using Servlet 6.0 async support</li>
 * <li>Session management for multiple client connections</li>
 * <li>Graceful shutdown support</li>
 * <li>Error handling and response formatting</li>
 * </ul>
 *
 * @author Christian Tzolov
 * @author Alexandros Pappas
 * @see McpServerTransportProvider
 * @see HttpServlet
 */

/**
 * @WebServlet 注解是 Jakarta Servlet API 提供的一种用于简化 servlet 配置的方法。它允许开发者通过注解的方式而非传统的 web.xml 描述符文件来定义 servlet 的基本信息和映射。
 * 作用
 * 	Servlet 注册和映射:
 * 		@WebServlet 用于将一个 Java 类注册为 servlet，并定义其 URL 映射。这样，容器（如 Tomcat、Jetty 等）在部署应用时，会自动识别和配置这个 servlet。
 * 	减少配置的复杂性:
 * 		使用注解可以减少在 web.xml 中手动配置的繁琐过程，直接通过注解参数定义 servlet 的各种属性，使代码更为简洁和集中。
 * 	配置初始化参数:
 * 		可以通过注解的属性配置 servlet 的初始化参数，简化了传统方式中的配置流程。
 *
 * 	使用方法
 * 		@WebServlet 注解可以用于一个类上，该类必须扩展 HttpServlet 类。常用的属性包括：
 * 			name: 用于指定 servlet 的名称。
 * 			urlPatterns 或 value: 指定该 servlet 可以处理的请求 URL 模式。value 是 urlPatterns 的一个别名。
 * 			initParams: 指定初始化参数。
 * 			loadOnStartup: 指定 servlet 的加载顺序。
 * 			asyncSupported: 指定 servlet 是否支持异步处理。
 */
@WebServlet(asyncSupported = true)
public class HttpServletSseServerTransportProvider extends HttpServlet implements McpServerTransportProvider {

	/** Logger for this class */
	private static final Logger logger = LoggerFactory.getLogger(HttpServletSseServerTransportProvider.class);

	public static final String UTF_8 = "UTF-8";

	public static final String APPLICATION_JSON = "application/json";

	public static final String FAILED_TO_SEND_ERROR_RESPONSE = "Failed to send error response: {}";

	/** Default endpoint path for SSE connections */
	public static final String DEFAULT_SSE_ENDPOINT = "/sse";

	/** Event type for regular messages */
	public static final String MESSAGE_EVENT_TYPE = "message";

	/** Event type for endpoint information */
	public static final String ENDPOINT_EVENT_TYPE = "endpoint";

	public static final String DEFAULT_BASE_URL = "";

	/** JSON object mapper for serialization/deserialization */
	private final ObjectMapper objectMapper;

	/** Base URL for the server transport */
	private final String baseUrl;

	/** The endpoint path for handling client messages */
	private final String messageEndpoint;

	/** The endpoint path for handling SSE connections */
	private final String sseEndpoint;

	/** Map of active client sessions, keyed by session ID */
	private final Map<String, McpServerSession> sessions = new ConcurrentHashMap<>();

	/** Flag indicating if the transport is in the process of shutting down */
	private final AtomicBoolean isClosing = new AtomicBoolean(false);

	/** Session factory for creating new sessions */
	private McpServerSession.Factory sessionFactory;

	/**
	 * Creates a new HttpServletSseServerTransportProvider instance with a custom SSE
	 * endpoint.
	 * @param objectMapper The JSON object mapper to use for message
	 * serialization/deserialization
	 * @param messageEndpoint The endpoint path where clients will send their messages
	 * @param sseEndpoint The endpoint path where clients will establish SSE connections
	 */
	public HttpServletSseServerTransportProvider(ObjectMapper objectMapper, String messageEndpoint,
			String sseEndpoint) {
		this(objectMapper, DEFAULT_BASE_URL, messageEndpoint, sseEndpoint);
	}

	/**
	 * Creates a new HttpServletSseServerTransportProvider instance with a custom SSE
	 * endpoint.
	 * @param objectMapper The JSON object mapper to use for message
	 * serialization/deserialization
	 * @param baseUrl The base URL for the server transport
	 * @param messageEndpoint The endpoint path where clients will send their messages
	 * @param sseEndpoint The endpoint path where clients will establish SSE connections
	 */
	public HttpServletSseServerTransportProvider(ObjectMapper objectMapper, String baseUrl, String messageEndpoint,
			String sseEndpoint) {
		this.objectMapper = objectMapper;
		this.baseUrl = baseUrl;
		this.messageEndpoint = messageEndpoint;
		this.sseEndpoint = sseEndpoint;
	}

	/**
	 * Creates a new HttpServletSseServerTransportProvider instance with the default SSE
	 * endpoint.
	 * @param objectMapper The JSON object mapper to use for message
	 * serialization/deserialization
	 * @param messageEndpoint The endpoint path where clients will send their messages
	 */
	public HttpServletSseServerTransportProvider(ObjectMapper objectMapper, String messageEndpoint) {
		this(objectMapper, messageEndpoint, DEFAULT_SSE_ENDPOINT);
	}

	/**
	 * Sets the session factory for creating new sessions.
	 * @param sessionFactory The session factory to use
	 */
	@Override
	public void setSessionFactory(McpServerSession.Factory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Broadcasts a notification to all connected clients.
	 * @param method The method name for the notification
	 * @param params The parameters for the notification
	 * @return A Mono that completes when the broadcast attempt is finished
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
				// onErrorComplete() 方法用于处理流中的错误信号
				//	onErrorComplete() 的作用
				//		错误转换为完成:
				//			当流中出现错误时，onErrorComplete() 会拦截该错误，并将其转换为一个完成信号。这样，流就会以正常完成的状态结束，而不是传播错误。
				//		错误处理逻辑简化:
				//			使用 onErrorComplete() 可以在某些情况下简化错误处理逻辑，特别是当你希望忽略某些错误并结束流处理时。
				//		控制流的终止方式:
				//			通过使用 onErrorComplete()，你可以控制流在遇到错误时的终止方式，让流以完成状态结束，而不是由于错误而中断。
				//	使用场景
				//		忽略某些错误:
				//			当特定类型的错误不需要被处理或传播时，使用 onErrorComplete() 可以让流继续正常结束。
				//		特定条件下的错误处理:
				//			结合条件逻辑，可以对特定的错误类型或条件进行处理，而忽略其他错误。
				.onErrorComplete())
			.then();
	}

	/**
	 * Handles GET requests to establish SSE connections.
	 * <p>
	 * This method sets up a new SSE connection when a client connects to the SSE
	 * endpoint. It configures the response headers for SSE, creates a new session, and
	 * sends the initial endpoint information to the client.
	 * @param request The HTTP servlet request
	 * @param response The HTTP servlet response
	 * @throws ServletException If a servlet-specific error occurs
	 * @throws IOException If an I/O error occurs
	 */

	//doGet 方法用于处理 HTTP GET 请求，并在特定情况下开启异步处理。
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String requestURI = request.getRequestURI();
		if (!requestURI.endsWith(sseEndpoint)) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		if (isClosing.get()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Server is shutting down");
			return;
		}

		// 设置响应头: 配置响应为 text/event-stream，设置为 UTF-8 编码，并设定适当的 HTTP 头以支持 Server-Sent Events (SSE)。
		response.setContentType("text/event-stream");
		response.setCharacterEncoding(UTF_8);
		response.setHeader("Cache-Control", "no-cache");
		response.setHeader("Connection", "keep-alive");
		response.setHeader("Access-Control-Allow-Origin", "*");

		String sessionId = UUID.randomUUID().toString();
		// 使用 request.startAsync() 开启异步处理，返回 AsyncContext 对象以管理异步操作。
		// 开启异步处理:
		//	request.startAsync() 方法用于启动异步处理模式，它允许请求在不阻塞 servlet 线程的情况下继续处理。这对于长时间运行的请求或需要保持连接打开的场景（如 SSE）非常有用。
		//返回 AsyncContext 对象:
		//	AsyncContext 是用于管理异步操作的核心对象。它提供了一些方法来控制异步请求的生命周期，比如完成、超时和调度异步任务。
		//  异步处理是 Java Servlet 3.0 引入的一项功能，旨在提高服务器的并发能力，特别是对于长时间运行的任务或需要保持连接的任务（如 SSE、WebSocket）而言。通过使用 AsyncContext，可以在不占用服务器线程的情况下处理复杂的异步请求逻辑，从而提升应用的响应能力和吞吐量。
		//AsyncContext 可以做什么
		//	控制异步请求生命周期:
		//		可以通过调用 asyncContext.complete() 来显式完成异步请求。
		//		设置异步请求的超时时间，通过 asyncContext.setTimeout(long timeout) 方法控制请求持续时间。
		//	调度异步任务:
		//		可以通过 asyncContext.start(Runnable runnable) 方法，将任务分配到容器管理的线程池中进行异步执行。
		//	获取原始请求和响应对象:
		//		asyncContext.getRequest() 和 asyncContext.getResponse() 可以用于获取原始的 HttpServletRequest 和 HttpServletResponse 对象，以便在异步操作中继续使用。
		AsyncContext asyncContext = request.startAsync();
		asyncContext.setTimeout(0);

		PrintWriter writer = response.getWriter();

		// Create a new session transport
		HttpServletMcpSessionTransport sessionTransport = new HttpServletMcpSessionTransport(sessionId, asyncContext,
				writer);

		// Create a new session using the session factory
		McpServerSession session = sessionFactory.create(sessionTransport);
		this.sessions.put(sessionId, session);

		// Send initial endpoint event
		this.sendEvent(writer, ENDPOINT_EVENT_TYPE, this.baseUrl + this.messageEndpoint + "?sessionId=" + sessionId);
	}

	/**
	 * Handles POST requests for client messages.
	 * <p>
	 * This method processes incoming messages from clients, routes them through the
	 * session handler, and sends back the appropriate response. It handles error cases
	 * and formats error responses according to the MCP specification.
	 * @param request The HTTP servlet request
	 * @param response The HTTP servlet response
	 * @throws ServletException If a servlet-specific error occurs
	 * @throws IOException If an I/O error occurs
	 */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		if (isClosing.get()) {
			response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Server is shutting down");
			return;
		}

		String requestURI = request.getRequestURI();
		if (!requestURI.endsWith(messageEndpoint)) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}

		// Get the session ID from the request parameter
		String sessionId = request.getParameter("sessionId");
		if (sessionId == null) {
			response.setContentType(APPLICATION_JSON);
			response.setCharacterEncoding(UTF_8);
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			String jsonError = objectMapper.writeValueAsString(new McpError("Session ID missing in message endpoint"));
			PrintWriter writer = response.getWriter();
			writer.write(jsonError);
			writer.flush();
			return;
		}

		// Get the session from the sessions map
		McpServerSession session = sessions.get(sessionId);
		if (session == null) {
			response.setContentType(APPLICATION_JSON);
			response.setCharacterEncoding(UTF_8);
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			String jsonError = objectMapper.writeValueAsString(new McpError("Session not found: " + sessionId));
			PrintWriter writer = response.getWriter();
			writer.write(jsonError);
			writer.flush();
			return;
		}

		try {
			BufferedReader reader = request.getReader();
			StringBuilder body = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				body.append(line);
			}

			McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(objectMapper, body.toString());

			// Process the message through the session's handle method
			session.handle(message).block(); // Block for Servlet compatibility

			response.setStatus(HttpServletResponse.SC_OK);
		}
		catch (Exception e) {
			logger.error("Error processing message: {}", e.getMessage());
			try {
				McpError mcpError = new McpError(e.getMessage());
				response.setContentType(APPLICATION_JSON);
				response.setCharacterEncoding(UTF_8);
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				String jsonError = objectMapper.writeValueAsString(mcpError);
				PrintWriter writer = response.getWriter();
				writer.write(jsonError);
				writer.flush();
			}
			catch (IOException ex) {
				logger.error(FAILED_TO_SEND_ERROR_RESPONSE, ex.getMessage());
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error processing message");
			}
		}
	}

	/**
	 * Initiates a graceful shutdown of the transport.
	 * <p>
	 * This method marks the transport as closing and closes all active client sessions.
	 * New connection attempts will be rejected during shutdown.
	 * @return A Mono that completes when all sessions have been closed
	 */
	@Override
	public Mono<Void> closeGracefully() {
		isClosing.set(true);
		logger.debug("Initiating graceful shutdown with {} active sessions", sessions.size());

		return Flux.fromIterable(sessions.values()).flatMap(McpServerSession::closeGracefully).then();
	}

	/**
	 * Sends an SSE event to a client.
	 * @param writer The writer to send the event through
	 * @param eventType The type of event (message or endpoint)
	 * @param data The event data
	 * @throws IOException If an error occurs while writing the event
	 */
	private void sendEvent(PrintWriter writer, String eventType, String data) throws IOException {
		writer.write("event: " + eventType + "\n");
		writer.write("data: " + data + "\n\n");
		writer.flush();

		if (writer.checkError()) {
			throw new IOException("Client disconnected");
		}
	}

	/**
	 * Cleans up resources when the servlet is being destroyed.
	 * <p>
	 * This method ensures a graceful shutdown by closing all client connections before
	 * calling the parent's destroy method.
	 */
	@Override
	public void destroy() {
		closeGracefully().block();
		super.destroy();
	}

	/**
	 * Implementation of McpServerTransport for HttpServlet SSE sessions. This class
	 * handles the transport-level communication for a specific client session.
	 */
	private class HttpServletMcpSessionTransport implements McpServerTransport {

		private final String sessionId;

		private final AsyncContext asyncContext;

		private final PrintWriter writer;

		/**
		 * Creates a new session transport with the specified ID and SSE writer.
		 * @param sessionId The unique identifier for this session
		 * @param asyncContext The async context for the session
		 * @param writer The writer for sending server events to the client
		 */
		HttpServletMcpSessionTransport(String sessionId, AsyncContext asyncContext, PrintWriter writer) {
			this.sessionId = sessionId;
			this.asyncContext = asyncContext;
			this.writer = writer;
			logger.debug("Session transport {} initialized with SSE writer", sessionId);
		}

		/**
		 * Sends a JSON-RPC message to the client through the SSE connection.
		 * @param message The JSON-RPC message to send
		 * @return A Mono that completes when the message has been sent
		 */
		@Override
		public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {
			return Mono.fromRunnable(() -> {
				try {
					String jsonText = objectMapper.writeValueAsString(message);
					sendEvent(writer, MESSAGE_EVENT_TYPE, jsonText);
					logger.debug("Message sent to session {}", sessionId);
				}
				catch (Exception e) {
					logger.error("Failed to send message to session {}: {}", sessionId, e.getMessage());
					sessions.remove(sessionId);
					asyncContext.complete();
				}
			});
		}

		/**
		 * Converts data from one type to another using the configured ObjectMapper.
		 * @param data The source data object to convert
		 * @param typeRef The target type reference
		 * @return The converted object of type T
		 * @param <T> The target type
		 */
		@Override
		public <T> T unmarshalFrom(Object data, TypeReference<T> typeRef) {
			return objectMapper.convertValue(data, typeRef);
		}

		/**
		 * Initiates a graceful shutdown of the transport.
		 * @return A Mono that completes when the shutdown is complete
		 */
		@Override
		public Mono<Void> closeGracefully() {
			return Mono.fromRunnable(() -> {
				logger.debug("Closing session transport: {}", sessionId);
				try {
					sessions.remove(sessionId);
					asyncContext.complete();
					logger.debug("Successfully completed async context for session {}", sessionId);
				}
				catch (Exception e) {
					logger.warn("Failed to complete async context for session {}: {}", sessionId, e.getMessage());
				}
			});
		}

		/**
		 * Closes the transport immediately.
		 */
		@Override
		public void close() {
			try {
				sessions.remove(sessionId);
				asyncContext.complete();
				logger.debug("Successfully completed async context for session {}", sessionId);
			}
			catch (Exception e) {
				logger.warn("Failed to complete async context for session {}: {}", sessionId, e.getMessage());
			}
		}

	}

	/**
	 * Creates a new Builder instance for configuring and creating instances of
	 * HttpServletSseServerTransportProvider.
	 * @return A new Builder instance
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for creating instances of HttpServletSseServerTransportProvider.
	 * <p>
	 * This builder provides a fluent API for configuring and creating instances of
	 * HttpServletSseServerTransportProvider with custom settings.
	 */
	public static class Builder {

		private ObjectMapper objectMapper = new ObjectMapper();

		private String baseUrl = DEFAULT_BASE_URL;

		private String messageEndpoint;

		private String sseEndpoint = DEFAULT_SSE_ENDPOINT;

		/**
		 * Sets the JSON object mapper to use for message serialization/deserialization.
		 * @param objectMapper The object mapper to use
		 * @return This builder instance for method chaining
		 */
		public Builder objectMapper(ObjectMapper objectMapper) {
			Assert.notNull(objectMapper, "ObjectMapper must not be null");
			this.objectMapper = objectMapper;
			return this;
		}

		/**
		 * Sets the base URL for the server transport.
		 * @param baseUrl The base URL to use
		 * @return This builder instance for method chaining
		 */
		public Builder baseUrl(String baseUrl) {
			Assert.notNull(baseUrl, "Base URL must not be null");
			this.baseUrl = baseUrl;
			return this;
		}

		/**
		 * Sets the endpoint path where clients will send their messages.
		 * @param messageEndpoint The message endpoint path
		 * @return This builder instance for method chaining
		 */
		public Builder messageEndpoint(String messageEndpoint) {
			Assert.hasText(messageEndpoint, "Message endpoint must not be empty");
			this.messageEndpoint = messageEndpoint;
			return this;
		}

		/**
		 * Sets the endpoint path where clients will establish SSE connections.
		 * <p>
		 * If not specified, the default value of {@link #DEFAULT_SSE_ENDPOINT} will be
		 * used.
		 * @param sseEndpoint The SSE endpoint path
		 * @return This builder instance for method chaining
		 */
		public Builder sseEndpoint(String sseEndpoint) {
			Assert.hasText(sseEndpoint, "SSE endpoint must not be empty");
			this.sseEndpoint = sseEndpoint;
			return this;
		}

		/**
		 * Builds a new instance of HttpServletSseServerTransportProvider with the
		 * configured settings.
		 * @return A new HttpServletSseServerTransportProvider instance
		 * @throws IllegalStateException if objectMapper or messageEndpoint is not set
		 */
		public HttpServletSseServerTransportProvider build() {
			if (objectMapper == null) {
				throw new IllegalStateException("ObjectMapper must be set");
			}
			if (messageEndpoint == null) {
				throw new IllegalStateException("MessageEndpoint must be set");
			}
			return new HttpServletSseServerTransportProvider(objectMapper, baseUrl, messageEndpoint, sseEndpoint);
		}

	}

}
