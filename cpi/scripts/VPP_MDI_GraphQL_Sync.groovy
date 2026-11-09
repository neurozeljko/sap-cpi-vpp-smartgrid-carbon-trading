package cpi.scripts

import com.sap.gateway.ip.core.customdev.util.Message

Message processData(Message message) {
    String feederId = message.getProperty("Prop_Feeder_ID")
    double totalMWh = message.getProperty("Prop_Total_MWh") as Double
    double totalCO2 = message.getProperty("Prop_Total_CO2_Offset") as Double

    String graphQLMutation = """
    mutation SyncVirtualPowerPlantAsset {
      updateGridEnergyAsset(
        input: {
          assetId: "${feederId}",
          operationalStatus: "ACTIVE_DISPATCHING",
          cumulativeDeliveredMWh: ${totalMWh.round(3)},
          verifiedCarbonOffsetTonnes: ${totalCO2.round(4)},
          lastSettlementAudit: "${new Date().format("yyyy-MM-dd'T'HH:mm:ss'Z'")}"
        }
      ) {
        assetId
        syncStatus
        acknowledgedTimestamp
      }
    }
    """.stripIndent().trim()

    message.setProperty("Prop_MDI_GraphQL_Query", graphQLMutation)
    message.setHeader("X-MDI-Operation", "AssetOperationalTelemetrySync")
    return message
}
