import java.util.Properties
import net.snowflake.ingest.streaming.InsertValidationResponse
import net.snowflake.ingest.streaming.OpenChannelRequest
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory
import java.net.URL
import java.util.Date
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

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
    lastOffset = ""
    batchCounter = 0
    for (record in records) {
        Map < String, Object > row = new HashMap <  > ();
        for (key in record.value.keySet()) {
            if (record.value[key] instanceof Date) {
                localDateTime = record.value[key].toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()
                row.put(key, localDateTime)
            } else {
                row.put(key, record.value[key]);
            }
        }
        // Insert the row with the current offset_token
        InsertValidationResponse response = channel1.insertRow(row, record.attributes['ss_offset']);
        if (response.hasErrors()) {
            // erroneous row
            throw response.getInsertErrors().get(0).getException();
        } else {
            lastOffset = record.attributes['ss_offset']
            batchCounter = batchCounter + 1
            sdc.output.write(record)
        }
    }
    // Check the offset_token registered in Snowflake to make sure everything is committed
    do {
        String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        if ( offsetTokenFromSnowflake != null && 
            offsetTokenFromSnowflake == lastOffset ) {
            sdc.log.info("SUCCESSFULLY inserted " + batchCounter + " rows");
            break ;
        }
    } while (true);
  
// Close the channel, the function internally will make sure everything is committed (or throw
    // an exception if there is any issue)
    channel1.close().get();
}
catch (e) {
    // Write a record to the error pipeline
    sdc.log.error(e.toString(), e)
}
