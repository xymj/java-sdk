/*
* Copyright 2024 - 2024 the original author or authors.
*/
package io.modelcontextprotocol.client.transport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A Server-Sent Events (SSE) client implementation using Java's Flow API for reactive
 * stream processing. This client establishes a connection to an SSE endpoint and
 * processes the incoming event stream, parsing SSE-formatted messages into structured
 * events.
 *
 * <p>
 * The client supports standard SSE event fields including:
 * <ul>
 * <li>event - The event type (defaults to "message" if not specified)</li>
 * <li>id - The event ID</li>
 * <li>data - The event payload data</li>
 * </ul>
 *
 * <p>
 * Events are delivered to a provided {@link SseEventHandler} which can process events and
 * handle any errors that occur during the connection.
 *
 * @author Christian Tzolov
 * @see SseEventHandler
 * @see SseEvent
 */
public class FlowSseClient {

	private final HttpClient httpClient;

	private final HttpRequest.Builder requestBuilder;

	/**
	 * Pattern to extract the data content from SSE data field lines. Matches lines
	 * starting with "data:" and captures the remaining content.
	 */
	private static final Pattern EVENT_DATA_PATTERN = Pattern.compile("^data:(.+)$", Pattern.MULTILINE);

	/**
	 * Pattern to extract the event ID from SSE id field lines. Matches lines starting
	 * with "id:" and captures the ID value.
	 */
	private static final Pattern EVENT_ID_PATTERN = Pattern.compile("^id:(.+)$", Pattern.MULTILINE);

	/**
	 * Pattern to extract the event type from SSE event field lines. Matches lines
	 * starting with "event:" and captures the event type.
	 */
	private static final Pattern EVENT_TYPE_PATTERN = Pattern.compile("^event:(.+)$", Pattern.MULTILINE);

	/**
	 * Record class representing a Server-Sent Event with its standard fields.
	 *
	 * @param id the event ID (may be null)
	 * @param type the event type (defaults to "message" if not specified in the stream)
	 * @param data the event payload data
	 */
	public static record SseEvent(String id, String type, String data) {
	}

	/**
	 * Interface for handling SSE events and errors. Implementations can process received
	 * events and handle any errors that occur during the SSE connection.
	 */
	public interface SseEventHandler {

		/**
		 * Called when an SSE event is received.
		 * @param event the received SSE event containing id, type, and data
		 */
		void onEvent(SseEvent event);

		/**
		 * Called when an error occurs during the SSE connection.
		 * @param error the error that occurred
		 */
		void onError(Throwable error);

	}

	/**
	 * Creates a new FlowSseClient with the specified HTTP client.
	 * @param httpClient the {@link HttpClient} instance to use for SSE connections
	 */
	public FlowSseClient(HttpClient httpClient) {
		this(httpClient, HttpRequest.newBuilder());
	}

	/**
	 * Creates a new FlowSseClient with the specified HTTP client and request builder.
	 * @param httpClient the {@link HttpClient} instance to use for SSE connections
	 * @param requestBuilder the {@link HttpRequest.Builder} to use for SSE requests
	 */
	public FlowSseClient(HttpClient httpClient, HttpRequest.Builder requestBuilder) {
		this.httpClient = httpClient;
		this.requestBuilder = requestBuilder;
	}

	/**
	 * Subscribes to an SSE endpoint and processes the event stream.
	 *
	 * <p>
	 * This method establishes a connection to the specified URL and begins processing the
	 * SSE stream. Events are parsed and delivered to the provided event handler. The
	 * connection remains active until either an error occurs or the server closes the
	 * connection.
	 * @param url the SSE endpoint URL to connect to
	 * @param eventHandler the handler that will receive SSE events and error
	 * notifications
	 * @throws RuntimeException if the connection fails with a non-200 status code
	 */
	public void subscribe(String url, SseEventHandler eventHandler) {
		HttpRequest request = this.requestBuilder.uri(URI.create(url))
			.header("Accept", "text/event-stream")
			.header("Cache-Control", "no-cache")
			.GET()
			.build();

		StringBuilder eventBuilder = new StringBuilder();
		AtomicReference<String> currentEventId = new AtomicReference<>();
		AtomicReference<String> currentEventType = new AtomicReference<>("message");

		//2、数据请求与处理：
		//	onSubscribe 中调用 subscription.request(Long.MAX_VALUE)，表示订阅者可以接收无限数据。
		//	发布者开始推送数据（逐行）。
		//	每次 onNext(String line) 处理一行数据后，调用 subscription.request(1) 请求下一行。

		// Flow.Subscriber 是 Java 流处理的核心接口，用于消费发布者（Publisher）推送的数据。它定义了四个方法：
		//	onSubscribe(Flow.Subscription subscription)：当订阅成功时调用，用于建立与发布者的连接。
		//	onNext(T item)：当发布者推送数据时调用。
		//	onError(Throwable throwable)：当发生错误时调用。
		//	onComplete()：当流结束时调用。
		Flow.Subscriber<String> lineSubscriber = new Flow.Subscriber<>() {

			// Flow.Subscription 是控制数据流速率的令牌，由 onSubscribe 方法传递给订阅者。它允许订阅者主动请求数据，避免发布者推送过快导致内存溢出。
			// 关键方法：
			//	request(long n)：请求 n 个数据项。
			//	cancel()：取消订阅。
			private Flow.Subscription subscription;

			// 在 onSubscribe 中不调用 request → 流不会触发 onNext
			@Override
			public void onSubscribe(Flow.Subscription subscription) {
				this.subscription = subscription;
				// 当订阅成功时，订阅者告诉发布者："我随时可以接收任意数量的数据"
				// 没有 request → 数据不会推送
				subscription.request(Long.MAX_VALUE);
			}

			//作用：每当发布者（Publisher）推送一个数据项时，onNext 方法会被调用一次，传递当前数据项。
			//职责：
			//	处理数据：对数据项进行解析、转换、存储等操作。
			//	控制流速：通过 subscription.request(n) 请求更多数据（可选）。


			//onNext 的触发条件如下：
			//	订阅成功后：
			//		调用 subscriber.onSubscribe(subscription) 建立订阅关系。
			//	发布者有数据可用时：
			//		发布者调用 subscriber.onNext(item) 推送数据。
			//	直到流结束或错误：
			//		流结束时调用 onComplete()。
			//		发生错误时调用 onError()。

			// 主动请求机制：
			//	onNext 的触发依赖于 subscription.request(n) 的调用。
			//	如果未调用 request，发布者不会推送数据。
			@Override
			public void onNext(String line) {
				if (line.isEmpty()) {
					// Empty line means end of event
					if (eventBuilder.length() > 0) {
						String eventData = eventBuilder.toString();
						SseEvent event = new SseEvent(currentEventId.get(), currentEventType.get(), eventData.trim());
						eventHandler.onEvent(event);
						eventBuilder.setLength(0);
					}
				}
				else {
					if (line.startsWith("data:")) {
						var matcher = EVENT_DATA_PATTERN.matcher(line);
						if (matcher.find()) {
							eventBuilder.append(matcher.group(1).trim()).append("\n");
						}
					}
					else if (line.startsWith("id:")) {
						var matcher = EVENT_ID_PATTERN.matcher(line);
						if (matcher.find()) {
							currentEventId.set(matcher.group(1).trim());
						}
					}
					else if (line.startsWith("event:")) {
						var matcher = EVENT_TYPE_PATTERN.matcher(line);
						if (matcher.find()) {
							currentEventType.set(matcher.group(1).trim());
						}
					}
				}

				// 3、流控制：
				//	如果 subscription.request(1) 被注释掉，订阅者将不会请求更多数据，导致流被暂停。
				//	subscription.request(Long.MAX_VALUE) 可能导致内存问题（如果数据量极大）。

				// 每次处理完一条数据后，请求下一条数据，控制流速为逐条处理。
				subscription.request(1);
			}

			@Override
			public void onError(Throwable throwable) {
				eventHandler.onError(throwable);
			}

			@Override
			public void onComplete() {
				// Handle any remaining event data
				if (eventBuilder.length() > 0) {
					String eventData = eventBuilder.toString();
					SseEvent event = new SseEvent(currentEventId.get(), currentEventType.get(), eventData.trim());
					eventHandler.onEvent(event);
				}
			}
		};

		// 在 Java 的 HttpClient API 中，HttpResponse.BodySubscribers.fromLineSubscriber(subscriber) 是一个 关键工具方法，用于将 HTTP 响应体 按行（Line-by-Line） 解析，并通过 Flow.Subscriber 处理每一行数据
		//1. 核心作用
		//该方法的作用是：
		//	将 HTTP 响应体转换为行流（Line Stream），逐行读取数据。
		//	通过 Flow.Subscriber 处理每一行数据，支持异步和响应式编程模型。
		//2. 使用场景
		//	适用于需要 逐行处理响应体 的场景，例如：
		//		Server-Sent Events (SSE)：实时接收服务器推送的事件流（如股票价格更新、日志流）。
		//		大文件分块处理：按行处理大文件（如 CSV、日志文件），避免一次性加载到内存。
		//		实时日志监控：逐行解析日志流，实时分析或展示。

		//工作原理
		//	响应体读取：
		//		客户端将 HTTP 响应体按行（以 \n 或 \r\n 分割）逐行读取。
		//	逐行推送：
		//		每一行数据通过 subscriber.onNext(line) 推送。
		//	流控制：
		//		通过 Flow.Subscription 控制数据流速率（避免内存溢出）。
		Function<Flow.Subscriber<String>, HttpResponse.BodySubscriber<Void>> subscriberFactory = subscriber -> HttpResponse.BodySubscribers
			// 参数subscriber：一个 Flow.Subscriber 实例，用于消费每一行数据。
			// 返回值一个 BodySubscriber<Void>，用于订阅 HTTP 响应体的行流。
			.fromLineSubscriber(subscriber);

		// 1、订阅建立：
		//	调用 httpClient.sendAsync(request, subscriberFactory) 发起 SSE 连接。
		//	当连接成功，发布者（HTTP 响应体）调用 lineSubscriber.onSubscribe(subscription)。
		CompletableFuture<HttpResponse<Void>> future = this.httpClient.sendAsync(request,
				info -> subscriberFactory.apply(lineSubscriber));

		future.thenAccept(response -> {
			int status = response.statusCode();
			if (status != 200 && status != 201 && status != 202 && status != 206) {
				throw new RuntimeException("Failed to connect to SSE stream. Unexpected status code: " + status);
			}
		}).exceptionally(throwable -> {
			eventHandler.onError(throwable);
			return null;
		});
	}

}
