package cpi.scripts

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput

Message processData(Message message) {
    def exception = message.getProperty("CamelExceptionCaught")
    String feederId = message.getProperty("Prop_Feeder_ID") ?: "UNKNOWN_FEEDER"
    String correlationId = message.getHeader("SAP_MplCorrelationId", String) ?: UUID.randomUUID().toString()
    String errorMsg = exception ? exception.getMessage() : "Energy settlement aggregation or financial transformation failed."

    def deadLetterPayload = [
        vppSettlementDeadLetterQueue: [
            incidentTrackingId: correlationId,
            gridFeederId: feederId,
            financialRiskStatus: "UNSETTLED_EXPOSURE",
            failureReason: errorMsg,
            quarantineAction: "ISOLATE_IN_SETTLEMENT_DLQ_AND_ALERT_TREASURY",
            timestamp: new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        ]
    ]

    message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(deadLetterPayload)))
    message.setHeader("Content-Type", "application/json")
    message.setHeader("X-Grid-Settlement-Status", "QUARANTINED")
    return message
}
