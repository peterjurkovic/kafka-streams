package com.nexmo.aggregator.deser;

import com.nexmo.aggregator.domain.SubmittedMessage;

public class SubmittedMessageDeser extends PojoDeser<SubmittedMessage> {

	public SubmittedMessageDeser() {
		super(SubmittedMessage.class);
	}

}