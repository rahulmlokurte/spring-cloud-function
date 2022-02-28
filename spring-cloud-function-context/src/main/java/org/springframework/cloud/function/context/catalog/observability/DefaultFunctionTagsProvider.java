package org.springframework.cloud.function.context.catalog.observability;

import io.micrometer.api.instrument.Tags;

public class DefaultFunctionTagsProvider implements FunctionTagsProvider {
	@Override
	public Tags getLowCardinalityTags(FunctionContext context) {
		return Tags.of(FunctionObservation.FunctionLowCardinalityTags.FUNCTION_NAME.of(context.getTargetFunction().getFunctionDefinition()));
	}
}
