package com.welflex.aws.dynamodb.repository.stream;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class OrderStreamRecordProcessorFactory implements IRecordProcessorFactory  {
    @Override
    public IRecordProcessor createProcessor() {
        return new OrderStreamRecordsProcessor();
    }
}
