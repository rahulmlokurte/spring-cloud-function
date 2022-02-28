/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.function.context.catalog.observability.tracing;

import java.util.ArrayList;
import java.util.List;

import io.micrometer.api.instrument.observation.Observation;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;
import org.springframework.cloud.function.context.catalog.observability.FunctionContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;

/**
 * Context.
 *
 * @author Marcin Grzejszczak
 * @since 4.0.0
 */
public class FunctionTracingObservationHandler implements TracingObservationHandler<FunctionContext> {

	private static final Log log = LogFactory.getLog(FunctionTracingObservationHandler.class);

	/**
	 * Using the literal "broker" until we come up with a better solution.
	 *
	 * <p>
	 * If the message originated from a binder (consumer binding), there will be different
	 * headers present (e.g. "KafkaHeaders.RECEIVED_TOPIC" Vs.
	 * "AmqpHeaders.CONSUMER_QUEUE" (unless the application removes them before sending).
	 * These don't represent the broker, rather a queue, and in any case the heuristics
	 * are not great. At least we might be able to tell if this is rabbit or not (ex how
	 * spring-rabbit works). We need to think this through before making an api, possibly
	 * experimenting.
	 *
	 * <p>
	 * If the app is outbound only (producer), there's no indication of what type the
	 * destination broker is. This may hint at a non-manual solution being overwriting the
	 * remoteServiceName later, similar to how servlet instrumentation lazy set
	 * "http.route".
	 */
	private static final String REMOTE_SERVICE_NAME = "broker";

	private static final String PARENT_SPAN_KEY = FunctionTracingObservationHandler.class.getName() + "_PARENT_SPAN";

	private static final String CHILD_SPAN_KEY = FunctionTracingObservationHandler.class.getName() + "_CHILD_SPAN";

	private static final String MESSAGES_AND_SPANS_KEY = FunctionTracingObservationHandler.class.getName() + "_MESSAGES_AND_SPANS";

	private final Tracer tracer;

	private final Propagator propagator;

	private final Propagator.Getter<MessageHeaderAccessor> getter;

	private final Propagator.Setter<MessageHeaderAccessor> setter;

	public FunctionTracingObservationHandler(Tracer tracer, Propagator propagator) {
		this.tracer = tracer;
		this.propagator = propagator;
		this.getter = new MessageHeaderPropagatorGetter();
		this.setter = new MessageHeaderPropagatorSetter();
	}

	@Override
	public void onStart(FunctionContext context) {
		Message<?> message = (Message<?>) context.getInput();
		MessageAndSpans invocationMessage = null;
		SimpleFunctionRegistry.FunctionInvocationWrapper targetFunction = context.getTargetFunction();
		Span span = null;
		if (message == null && targetFunction.isSupplier()) { // Supplier
			if (log.isDebugEnabled()) {
				log.debug("Creating a span for a supplier");
			}
			span = this.tracer.nextSpan();
		}
		else {
			if (log.isDebugEnabled()) {
				log.debug("Will retrieve the tracing headers from the message");
			}
			invocationMessage = wrapInputMessage(context, message);
			if (log.isDebugEnabled()) {
				log.debug("Wrapped input msg " + invocationMessage);
			}
			span = invocationMessage.childSpan;
		}
	}

	/**
	 * Wraps the given input message with tracing headers and returns a corresponding
	 * span.
	 * @param message - message to wrap
	 * @return a tuple with the wrapped message and a corresponding span
	 */
	private MessageAndSpans wrapInputMessage(FunctionContext context, Message<?> message) {
		MessageHeaderAccessor headers = mutableHeaderAccessor(message);
		Span.Builder consumerSpanBuilder = this.propagator.extract(headers, this.getter);
		Span consumerSpan = consumerSpan(context, consumerSpanBuilder);
		if (log.isDebugEnabled()) {
			log.debug("Built a consumer span " + consumerSpan);
		}
		Span childSpan = consumerSpan.name(context.getContextualName());
		clearTracingHeaders(headers);
		context.put(PARENT_SPAN_KEY, consumerSpan);
		context.put(CHILD_SPAN_KEY, childSpan);
		if (message instanceof ErrorMessage) {
			return new MessageAndSpans(new ErrorMessage((Throwable) message.getPayload(), headers.getMessageHeaders()),
				consumerSpan, childSpan);
		}
		headers.setImmutable();
		return new MessageAndSpans(new GenericMessage<>(message.getPayload(), headers.getMessageHeaders()),
			consumerSpan, childSpan);
	}

	private Span consumerSpan(FunctionContext context, Span.Builder consumerSpanBuilder) {
		// TODO: Add this as a documented span
		consumerSpanBuilder.kind(Span.Kind.CONSUMER).name("handle");
		consumerSpanBuilder.remoteServiceName(REMOTE_SERVICE_NAME);
		// this is the consumer part of the producer->consumer mechanism
		Span consumerSpan = consumerSpanBuilder.start();
		tagSpan(context, consumerSpan);
		// we're ending this immediately just to have a properly nested graph
		consumerSpan.end();
		return consumerSpan;
	}

	private MessageHeaderAccessor mutableHeaderAccessor(Message<?> message) {
		MessageHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, MessageHeaderAccessor.class);
		if (accessor != null && accessor.isMutable()) {
			return accessor;
		}
		MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
		headers.setLeaveMutable(true);
		return headers;
	}

	private void clearTracingHeaders(MessageHeaderAccessor headers) {
		List<String> keysToRemove = new ArrayList<>(this.propagator.fields());
		keysToRemove.add(Span.class.getName());
		keysToRemove.add("traceHandlerParentSpan");
		MessageHeaderPropagatorSetter.removeAnyTraceHeaders(headers, keysToRemove);
	}

	@Override
	public Tracer getTracer() {
		return this.tracer;
	}

	@Override
	public boolean supportsContext(Observation.Context context) {
		return context instanceof FunctionContext && (((FunctionContext) context).getInput() instanceof Message<?>);
	}

	private static class MessageAndSpan {

		final Message msg;

		final Span span;

		MessageAndSpan(Message msg, Span span) {
			this.msg = msg;
			this.span = span;
		}

		@Override
		public String toString() {
			return "MessageAndSpan{" + "msg=" + this.msg + ", span=" + this.span + '}';
		}

	}

	private static class MessageAndSpans {

		final Message msg;

		final Span parentSpan;

		final Span childSpan;

		MessageAndSpans(Message msg, Span parentSpan, Span childSpan) {
			this.msg = msg;
			this.parentSpan = parentSpan;
			this.childSpan = childSpan;
		}

		@Override
		public String toString() {
			return "MessageAndSpans{" + "msg=" + msg + ", parentSpan=" + parentSpan + ", childSpan=" + childSpan + '}';
		}

	}
}
