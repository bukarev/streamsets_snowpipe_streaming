import java.net.URL
import java.util.Date
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.Properties
import net.snowflake.ingest.streaming.OpenChannelRequest
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory
import net.snowflake.ingest.streaming.InsertValidationResponse

dbName = sdc.pipelineParameters()['p_sfl_db']
schemaName = sdc.pipelineParameters()['p_sfl_schema']
tableName = sdc.pipelineParameters()['p_sfl_table']
clientName = sdc.userParams['job_id']
channelName = "Streamsets:" + tableName  

Properties props = new Properties(); 
props.put("user", sdc.pipelineParameters()['p_sfl_user'])
props.put("url", sdc.pipelineParameters()['p_sfl_url'])
props.put("private_key", sdc.userParams['snowflake_pk'])
props.put("port", sdc.pipelineParameters()['p_sfl_port'])

url = new URL(sdc.pipelineParameters()['p_sfl_url'])
props.put("host", url.host)

props.put("scheme", "https")
props.put("role", sdc.pipelineParameters()['p_sfl_role'])

// Create a streaming ingest client
try (
    SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder(clientName).setProperties(props).build()
) {
    OpenChannelRequest request1 = OpenChannelRequest.builder(channelName)
        .setDBName(dbName)
        .setSchemaName(schemaName)
        .setTableName(tableName)
        .setOnErrorOption(
        OpenChannelRequest.OnErrorOption.CONTINUE
    )
        .build();
    // Open a streaming ingest channel from the given client
    SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);
    // Insert rows into the channel (Using insertRows API)
    records = sdc.records
    firstOffset = ""
    lastOffset = ""
    batchCounter = 0
    List < Map < String, Object >> insertRecords = []
    for (record in records) {
        if (!this.properties.containsKey('row')) {
            firstOffset = record.attributes['ss_offset']
        }
        lastOffset = record.attributes['ss_offset']
        Map < String, Object > row = new HashMap < > ();
        for (key in record.value.keySet()) {
            if (record.value[key] instanceof Date) {
                localDateTime = record.value[key].toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()
                row.put(key, localDateTime)
            } else {
                row.put(key, record.value[key])
            }
        }
        insertRecords.add(row)
        batchCounter = batchCounter + 1
    }
    // Insert the row with the current offset_token
    InsertValidationResponse response = channel1.insertRows(insertRecords, firstOffset, lastOffset)
    List < Integer > errorIndexes = []
    if (response.hasErrors()) {
        // erroneous rows
        for (insError in response.getInsertErrors()) {
            errorIndexes.add(insError.getRowIndex().toInteger())
            sdc.error.write(sdc.records[insError.getRowIndex().toInteger()], insError.getException().toString())
        }
    }
    // Check the offset_token registered in Snowflake to make sure everything is committed
    recordsInserted = records.size() - errorIndexes.size()
    endTime = System.currentTimeMillis() + (sdc.userParams['confirmation_timeout_s'].toInteger() * 1000)
    do {
        String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        if (
            offsetTokenFromSnowflake != null && offsetTokenFromSnowflake == lastOffset
        ) {
            sdc.log.info("SUCCESSFULLY inserted " + recordsInserted + " rows and sent " + errorIndexes.size() 
                + " to error.");
            break;
        }
    } while (System.currentTimeMillis() < endTime);
    channel1.close().get();
}
catch (e) {
    // Write a record to the error pipeline
    sdc.log.error(e.toString(), e)
}
