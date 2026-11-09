package cpi.scripts

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonOutput

Message processData(Message message) {
    String feederId = message.getProperty("Prop_Feeder_ID")
    double totalEUR = message.getProperty("Prop_Total_EUR") as Double
    double totalMWh = message.getProperty("Prop_Total_MWh") as Double
    int tradeCount = message.getProperty("Prop_Trade_Count") as Integer
    String correlationId = message.getProperty("Prop_Correlation_ID") ?: UUID.randomUUID().toString()

    String postingDate = new Date().format("yyyy-MM-dd")
    String headerText = "VPP 15m Settlement Feeder ${feederId} [${tradeCount} Trades]"

    def s4JournalEntry = [
        CompanyCode: "1010",
        DocumentDate: postingDate,
        PostingDate: postingDate,
        AccountingDocumentType: "SA",
        DocumentHeaderText: headerText.take(25),
        CreatedByUser: "BTP_VPP_INTEGRATION",
        to_Item: [
            [
                ReferenceDocumentItem: "1",
                GLAccount: "0011001000",
                AmountInTransactionCurrency: totalEUR,
                TransactionCurrency: "EUR",
                DebitCreditCode: "S",
                DocumentItemText: "VPP Net Inflow ${totalMWh} MWh"
            ],
            [
                ReferenceDocumentItem: "2",
                GLAccount: "0080001000",
                AmountInTransactionCurrency: -totalEUR,
                TransactionCurrency: "EUR",
                DebitCreditCode: "H",
                DocumentItemText: "Carbon-Free Spot Settlement"
            ]
        ],
        auditMetadata: [
            correlationTrackingId: correlationId,
            clearingFeederNode: feederId,
            tradeCountAggregated: tradeCount,
            settlementTimestamp: new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'")
        ]
    ]

    message.setBody(JsonOutput.prettyPrint(JsonOutput.toJson(s4JournalEntry)))
    message.setHeader("Content-Type", "application/json")
    message.setHeader("X-CleanCore-Target", "S4HANA_FINANCE_ODATA_V4")
    return message
}
