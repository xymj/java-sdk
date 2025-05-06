/*
 * Copyright 2024-2024 the original author or authors.
 */

package io.modelcontextprotocol.server.transport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.JSONRPCMessage;
import io.modelcontextprotocol.spec.McpServerSession;
import io.modelcontextprotocol.spec.McpServerTransport;
import io.modelcontextprotocol.spec.McpServerTransportProvider;
import io.modelcontextprotocol.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Implementation of the MCP Stdio transport provider for servers that communicates using
 * standard input/output streams. Messages are exchanged as newline-delimited JSON-RPC
 * messages over stdin/stdout, with errors and debug information sent to stderr.
 *
 * @author Christian Tzolov
 */
public class StdioServerTransportProvider implements McpServerTransportProvider {

	private static final Logger logger = LoggerFactory.getLogger(StdioServerTransportProvider.class);

	private final ObjectMapper objectMapper;

	private final InputStream inputStream;

	private final OutputStream outputStream;

	private McpServerSession session;

	private final AtomicBoolean isClosing = new AtomicBoolean(false);

	private final Sinks.One<Void> inboundReady = Sinks.one();

	/**
	 * Creates a new StdioServerTransportProvider with a default ObjectMapper and System
	 * streams.
	 */
	public StdioServerTransportProvider() {
		this(new ObjectMapper());
	}

	/**
	 * Creates a new StdioServerTransportProvider with the specified ObjectMapper and
	 * System streams.
	 * @param objectMapper The ObjectMapper to use for JSON serialization/deserialization
	 */
	public StdioServerTransportProvider(ObjectMapper objectMapper) {
		this(objectMapper, System.in, System.out);
	}

	/**
	 * Creates a new StdioServerTransportProvider with the specified ObjectMapper and
	 * streams.
	 * @param objectMapper The ObjectMapper to use for JSON serialization/deserialization
	 * @param inputStream The input stream to read from
	 * @param outputStream The output stream to write to
	 */
	public StdioServerTransportProvider(ObjectMapper objectMapper, InputStream inputStream, OutputStream outputStream) {
		Assert.notNull(objectMapper, "The ObjectMapper can not be null");
		Assert.notNull(inputStream, "The InputStream can not be null");
		Assert.notNull(outputStream, "The OutputStream can not be null");

		this.objectMapper = objectMapper;
		this.inputStream = inputStream;
		this.outputStream = outputStream;
	}

	@Override
	public void setSessionFactory(McpServerSession.Factory sessionFactory) {
		// Create a single session for the stdio connection
		var transport = new StdioMcpSessionTransport();
		this.session = sessionFactory.create(transport);
		transport.initProcessing();
	}

	@Override
	public Mono<Void> notifyClients(String method, Object params) {
		if (this.session == null) {
			return Mono.error(new McpError("No session to close"));
		}
		return this.session.sendNotification(method, params)
			.doOnError(e -> logger.error("Failed to send notification: {}", e.getMessage()));
	}

	@Override
	public Mono<Void> closeGracefully() {
		if (this.session == null) {
			return Mono.empty();
		}
		return this.session.closeGracefully();
	}

	/**
	 * Implementation of McpServerTransport for the stdio session.
	 */
	private class StdioMcpSessionTransport implements McpServerTransport {

		private final Sinks.Many<JSONRPCMessage> inboundSink;

		private final Sinks.Many<JSONRPCMessage> outboundSink;

		private final AtomicBoolean isStarted = new AtomicBoolean(false);

		/** Scheduler for handling inbound messages */
		private Scheduler inboundScheduler;

		/** Scheduler for handling outbound messages */
		private Scheduler outboundScheduler;

		private final Sinks.One<Void> outboundReady = Sinks.one();

		public StdioMcpSessionTransport() {

			this.inboundSink = Sinks.many().unicast().onBackpressureBuffer();
			this.outboundSink = Sinks.many().unicast().onBackpressureBuffer();

			// Use bounded schedulers for better resource management
			this.inboundScheduler = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor(),
					"stdio-inbound");
			this.outboundScheduler = Schedulers.fromExecutorService(Executors.newSingleThreadExecutor(),
					"stdio-outbound");
		}

		@Override
		public Mono<Void> sendMessage(McpSchema.JSONRPCMessage message) {

			// Mono.zip() 是一个方法，它接收两个或更多的 Mono 对象，并且等待所有的这些 Mono 对象都成功完成后，将它们的结果组合起来。在这个特定的例子中：
			//inboundReady.asMono()：表示一个异步操作，当输入准备好后会发出信号。
			//outboundReady.asMono()：表示另一个异步操作，当输出准备好后会发出信号。
			//Mono.zip() 等待这两个异步操作都完成（即输入和输出都准备好），之后才会继续执行后续的操作。

			//then() 方法是 Mono 类的一个方法，它允许你在当前 Mono 操作完成后执行另一个 Mono 操作。这里的 Mono.defer() 是一个延迟创建 Mono 的方式，直到实际需要的时候才去创建。具体来说，在这个例子中：
			//
			//如果 outboundSink.tryEmitNext(message) 成功，则返回一个空的 Mono (Mono.empty())，表示消息已经成功发送。
			//否则，抛出一个运行时异常 (RuntimeException) 表示消息无法入队。
			return Mono.zip(inboundReady.asMono(), outboundReady.asMono()).then(Mono.defer(() -> {
				if (outboundSink.tryEmitNext(message).isSuccess()) {
					return Mono.empty();
				}
				else {
					return Mono.error(new RuntimeException("Failed to enqueue message"));
				}
			}));
		}

		@Override
		public <T> T unmarshalFrom(Object data, TypeReference<T> typeRef) {
			return objectMapper.convertValue(data, typeRef);
		}

		@Override
		public Mono<Void> closeGracefully() {
			return Mono.fromRunnable(() -> {
				isClosing.set(true);
				logger.debug("Session transport closing gracefully");
				inboundSink.tryEmitComplete();
			});
		}

		@Override
		public void close() {
			isClosing.set(true);
			logger.debug("Session transport closed");
		}

		private void initProcessing() {
			handleIncomingMessages();
			startInboundProcessing();
			startOutboundProcessing();
		}

		private void handleIncomingMessages() {
			//doOnTerminate() 方法是一个钩子，它会在流的生命周期即将结束时触发。具体来说，doOnTerminate() 会在以下两种情况下被调用：
			//	正常完成: 当流正常完成（即所有元素都已处理并发射完成信号）时触发。
			//	异常终止: 当流因错误而终止时触发。
			//	doOnTerminate() 不会区分流是正常结束还是异常结束，也就是说，它不关心流的终止原因，只关注终止的事实。
			this.inboundSink.asFlux().flatMap(message -> session.handle(message)).doOnTerminate(() -> {
				// The outbound processing will dispose its scheduler upon completion
				// inboundSink.tryEmitComplete() 是 Reactor 中 Sinks.Many 接口提供的方法之一，用于向下游信号发射一个完成信号。这是一个重要的操作，因为在反应式流中，完成信号表示数据流的终止。
				//	tryEmitComplete() 的作用
				//		发射完成信号:
				//			tryEmitComplete() 用于指示数据流已经到达终点，没有更多的数据会被发射。下游的消费者会收到此信号，并可以执行相应的终止逻辑。
				//		终止流:
				//			在调用 tryEmitComplete() 之后，Sinks.Many 不会接收新的数据发射操作。任何后续的 tryEmitNext() 调用都将被忽略，或者返回失败状态。
				//		资源清理:
				//			对于流的下游，完成信号可以触发资源清理操作，例如关闭连接、释放内存或其他清理工作。这使得在异步和并行处理场景中，能够有效地管理和释放资源。
				//		使用场景
				//			数据流结束: 在所有数据项被处理之后调用 tryEmitComplete()，通知下游没有更多的数据，这通常用于文件读取结束、数据流处理结束等场景。
				//			主动终止流: 在某些条件下，决定提前终止流的处理，比如检测到某种终止条件或者发生错误后不再继续处理。
				this.outboundSink.tryEmitComplete();

				//inboundScheduler.dispose() 的作用
				//	释放资源:
				//		dispose() 方法用于释放调度器所用的资源，通常是在调度器不再需要时调用的。释放资源的动作可以防止资源泄漏，特别是在长时间运行的应用或服务器中，这样做可以确保应用的资源使用更高效。
				//	终止调度器:
				//		调用 dispose() 后，调度器将不会接受新的任务。已经在执行队列中的任务可能会被正常执行完毕，但新的任务提交将被拒绝。
				this.inboundScheduler.dispose();
			}).subscribe();
		}

		/**
		 * Starts the inbound processing thread that reads JSON-RPC messages from stdin.
		 * Messages are deserialized and passed to the session for handling.
		 */
		private void startInboundProcessing() {
			if (isStarted.compareAndSet(false, true)) {
				this.inboundScheduler.schedule(() -> {
					inboundReady.tryEmitValue(null);
					BufferedReader reader = null;
					try {
						reader = new BufferedReader(new InputStreamReader(inputStream));
						while (!isClosing.get()) {
							try {
								String line = reader.readLine();
								if (line == null || isClosing.get()) {
									break;
								}

								logger.debug("Received JSON message: {}", line);

								try {
									McpSchema.JSONRPCMessage message = McpSchema.deserializeJsonRpcMessage(objectMapper,
											line);
									if (!this.inboundSink.tryEmitNext(message).isSuccess()) {
										// logIfNotClosing("Failed to enqueue message");
										break;
									}

								}
								catch (Exception e) {
									logIfNotClosing("Error processing inbound message", e);
									break;
								}
							}
							catch (IOException e) {
								logIfNotClosing("Error reading from stdin", e);
								break;
							}
						}
					}
					catch (Exception e) {
						logIfNotClosing("Error in inbound processing", e);
					}
					finally {
						isClosing.set(true);
						if (session != null) {
							session.close();
						}
						inboundSink.tryEmitComplete();
					}
				});
			}
		}

		/**
		 * Starts the outbound processing thread that writes JSON-RPC messages to stdout.
		 * Messages are serialized to JSON and written with a newline delimiter.
		 */
		private void startOutboundProcessing() {
			Function<Flux<JSONRPCMessage>, Flux<JSONRPCMessage>> outboundConsumer = messages -> messages // @formatter:off
				 .doOnSubscribe(subscription -> outboundReady.tryEmitValue(null))
				 .publishOn(outboundScheduler)
				 // handle 方法和 sink 的作用
				 //sink 的作用:
				 //		sink 是 SynchronousSink 的一个实例，用于在 handle 操作符中发射（emit）元素。
				 //		它允许你对每个元素执行自定义逻辑，然后通过调用 sink.next(value) 发射处理后的元素。
				 //		你还可以通过 sink.error(error) 发射错误信号，或者通过 sink.complete() 发射完成信号。
				 //handle 方法的功能:
				 //		handle 是一个操作符，用于提供对每个流元素的自定义处理逻辑。它结合了 map 和 filter 的功能，因为可以选择性地发射零个或多个元素。
				 //		在你的代码中，它被用来将 JSONRPCMessage 对象转换成 JSON 字符串，并将其写入 outputStream。如果成功写入，则通过 sink.next(message) 发射处理过的消息。如果发生错误，则通过 sink.error() 发射错误信号。
				 .handle((message, sink) -> {
					 if (message != null && !isClosing.get()) {
						 try {
							 String jsonMessage = objectMapper.writeValueAsString(message);
							 // Escape any embedded newlines in the JSON message as per spec
							 jsonMessage = jsonMessage.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n");
	
							 synchronized (outputStream) {
								 outputStream.write(jsonMessage.getBytes(StandardCharsets.UTF_8));
								 outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
								 outputStream.flush();
							 }
							 sink.next(message);
						 }
						 catch (IOException e) {
							 if (!isClosing.get()) {
								 logger.error("Error writing message", e);
								 sink.error(new RuntimeException(e));
							 }
							 else {
								 logger.debug("Stream closed during shutdown", e);
							 }
						 }
					 }
					 else if (isClosing.get()) {
						 sink.complete();
					 }
				 })
				 .doOnComplete(() -> {
					 isClosing.set(true);
					 outboundScheduler.dispose();
				 })
				 .doOnError(e -> {
					 if (!isClosing.get()) {
						 logger.error("Error in outbound processing", e);
						 isClosing.set(true);
						 outboundScheduler.dispose();
					 }
				 })
				 //具体作用
				 //	类型转换:
				 //		这段代码通过 map 操作符对流中的每个元素进行操作，将其转换为 JSONRPCMessage 类型。
				 //		由于 handle 操作符返回的流元素类型不明确，使用这一步可以确保后续操作处理的是明确的 JSONRPCMessage 类型。
				 //	数据流操作:
				 //		map 是一个常见的流操作符，用于将输入流中的元素转换为另一种形式的元素，并生成一个新的流。这种转换是通过提供的函数（这里是 msg -> (JSONRPCMessage) msg）进行的。
				 //在这个例子中，map 的转换作用似乎是冗余的，或者是为了确保流中的每个元素在类型上是 JSONRPCMessage。如果前面的流处理已经保证了元素类型一致，这一步可能没有显式的必要。
				 .map(msg -> (JSONRPCMessage) msg);
	
				 outboundConsumer.apply(outboundSink.asFlux()).subscribe();
		 } // @formatter:on

		private void logIfNotClosing(String message, Exception e) {
			if (!isClosing.get()) {
				logger.error(message, e);
			}
		}

	}

}
