package com.kafka.perf.audit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class AuditOutcomeEventTest {

    @Test
    public void deserializesValidOutcomeEvent() {
        String json = """
            {
              "eventId":"abc",
              "outcome":"REPLAY_OBSERVED",
              "consumerGroup":"perf-group",
              "sourceTopic":"events",
              "firstSeenAt":"2026-03-20T00:00:00Z",
              "lastSeenAt":"2026-03-20T00:00:05Z",
              "observedAt":"2026-03-20T00:00:05Z",
              "recordCount":10,
              "replayCount":2,
              "timeoutCount":1,
              "partitionRanges":[
                {"partition":0,"offsetMin":100,"offsetMax":104,"recordCount":5},
                {"partition":1,"offsetMin":200,"offsetMax":204,"recordCount":5}
              ]
            }
            """;

        AuditOutcomeEvent event = AuditOutcomeEvent.fromJson(json);

        assertEquals("abc", event.eventId);
        assertEquals("REPLAY_OBSERVED", event.outcome);
        assertEquals("perf-group", event.consumerGroup);
        assertEquals("events", event.sourceTopic);
        assertEquals(10, event.recordCount);
        assertEquals(2, event.replayCount);
        assertEquals(1, event.timeoutCount);
        assertNotNull(event.partitionRanges);
        assertEquals(2, event.partitionRanges.size());
        assertEquals(0, event.partitionRanges.get(0).partition);
    }
}
