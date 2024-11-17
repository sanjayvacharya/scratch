package com.welflex.aws.dynamodb.repository.stream;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import java.util.Map;

public class OrderStreamRecordsProcessor implements IRecordProcessor {
    @Override
    public void initialize(InitializationInput initializationInput) {

    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record : processRecordsInput.getRecords()) {

            if (record instanceof RecordAdapter) {
                com.amazonaws.services.dynamodbv2.model.Record streamRecord
                        = ((RecordAdapter) record).getInternalObject();
                if ("INSERT".equals(streamRecord.getEventName())) {
                    Map<String, AttributeValue> values = streamRecord.getDynamodb()
                            .getNewImage();
                    int totalPrice = Integer.parseInt(values.get("totalPrice").getN());
                        System.out.println("Stream Order:" + values);

                }
            }
        }
        checkPoint(processRecordsInput.getCheckpointer());
    }

    private void checkPoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {

    }
}
