package cpi.scripts

import com.sap.gateway.ip.core.customdev.util.Message
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

Message processData(Message message) {
    def reader = message.getBody(java.io.Reader)
    def jsonSlurper = new JsonSlurper()
    def payload = jsonSlurper.parse(reader)

    String feederId = payload.gridFeederId ?: "FEEDER-REGIONAL-DE-01"
    String settlementWindow = payload.settlementWindow ?: "2026-11-09T13:00:00Z/13:15:00Z"
    def microTransactions = payload.energyTrades ?: []

    if (microTransactions.isEmpty()) {
        throw new IllegalArgumentException("VPP_SETTLEMENT_ERROR: Energy trade batch is empty.")
    }

    double totalMWhDelivered = 0.0
    double totalGrossSettlementEUR = 0.0
    double totalCarbonCreditsTonnes = 0.0
    int totalTradeCount = microTransactions.size()

    microTransactions.each { trade ->
        double mwh = (trade.megawattHours ?: 0.0) as Double
        double rateEur = (trade.spotPriceEurPerMwh ?: 0.0) as Double
        double co2Factor = (trade.carbonOffsetFactor ?: 0.45) as Double

        totalMWhDelivered += mwh
        totalGrossSettlementEUR += (mwh * rateEur)
        totalCarbonCreditsTonnes += (mwh * co2Factor)
    }

    double volumeWeightedAveragePrice = totalMWhDelivered > 0 ? (totalGrossSettlementEUR / totalMWhDelivered) : 0.0

    message.setProperty("Prop_Total_MWh", totalMWhDelivered)
    message.setProperty("Prop_Total_EUR", totalGrossSettlementEUR.round(2))
    message.setProperty("Prop_Total_CO2_Offset", totalCarbonCreditsTonnes.round(4))
    message.setProperty("Prop_VWAP_EUR", volumeWeightedAveragePrice.round(2))
    message.setProperty("Prop_Trade_Count", totalTradeCount)
    message.setProperty("Prop_Feeder_ID", feederId)

    def aggregatedSummary = [
        vppAggregationSummary: [
            gridFeederId: feederId,
            settlementWindow: settlementWindow,
            tradeCount: totalTradeCount,
            totalEnergyMWh: totalMWhDelivered.round(3),
            totalSettlementAmountEUR: totalGrossSettlementEUR.round(2),
            vwapEurPerMwh: volumeWeightedAveragePrice.round(2),
            netCarbonOffsetTonnes: totalCarbonCreditsTonnes.round(4),
            status: "AGGREGATED_READY_FOR_POSTING"
        ]
    ]

    message.setBody(JsonOutput.toJson(aggregatedSummary))
    return message
}
