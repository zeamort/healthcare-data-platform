# Kinesis Data Stream — real-time ingestion of clinical events.
# Used to simulate streaming patient data after the batch cutoff date.

resource "aws_kinesis_stream" "clinical_events" {
  name             = "${local.name_prefix}-clinical-events"
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = { Name = "${local.name_prefix}-clinical-events" }
}
