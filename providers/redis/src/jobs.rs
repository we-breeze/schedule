use crate::RedisProvider;
use async_trait::async_trait;
use brz_schedule::*;
use serde_json::{Value, json};

#[async_trait]
impl JobProvider for RedisProvider {
    async fn submit(&self, job: Job) -> Result<SubmitResult> {
        job.validate()?;
        let result = self
            .operation(
                "submit",
                json!({"job":wire_job(&job)?,"capacity":self.config.capacity}),
            )
            .await?;
        match result.as_str() {
            Some("inserted") => Ok(SubmitResult::Inserted),
            Some("existing") => Ok(SubmitResult::Existing),
            Some("overlap") => Ok(SubmitResult::OverlapSkipped),
            _ => Err(Error::Provider("invalid submit response".into())),
        }
    }
    async fn get(&self, id: &str) -> Result<Option<JobRecord>> {
        let mut value = self.operation("get", json!({"id":id})).await?;
        let mut result = None;
        if !value.is_null() {
            decode_record(&mut value)?;
            // Option<Value> normally collapses JSON null into None. The wire
            // string distinguishes a successful null result from no result.
            if value["status"] == "Succeeded" {
                result = Some(value["result"].clone());
            }
        }
        let mut record: Option<JobRecord> = serde_json::from_value(value)?;
        if let Some(record) = &mut record {
            record.result = result;
        }
        Ok(record)
    }
    async fn claim(
        &self,
        owner: &str,
        kinds: &[String],
        limit: usize,
        lease_ms: u64,
    ) -> Result<Vec<Claim>> {
        validate_lease(lease_ms)?;
        if owner.is_empty() {
            return Err(Error::Invalid("invalid owner".into()));
        }
        let limit = limit.min(self.config.capacity);
        let mut value = self.operation("claim", json!({"owner":owner,"kinds":kinds,"limit":limit,
            "lease_ms":lease_ms,"nonce":uuid::Uuid::new_v4().to_string(),"capacity":self.config.capacity,
            "retention":self.config.retained_terminal})).await?;
        for claim in value
            .as_array_mut()
            .ok_or_else(|| Error::Provider("invalid claims response".into()))?
        {
            decode_record(&mut claim["record"])?;
        }
        Ok(serde_json::from_value(value)?)
    }
    async fn renew(&self, id: &str, token: &str, lease_ms: u64) -> Result<bool> {
        validate_lease(lease_ms)?;
        Ok(serde_json::from_value(
            self.operation("renew", json!({"id":id,"token":token,"lease_ms":lease_ms}))
                .await?,
        )?)
    }
    async fn finish(&self, id: &str, token: &str, completion: Completion) -> Result<bool> {
        let continuation_payload = match &completion {
            Completion::ContinueAt {
                payload: Some(payload),
                ..
            } => Some(serde_json::to_string(payload)?),
            _ => None,
        };
        let mut completion = serde_json::to_value(completion)?;
        if let Some(value) = completion.get_mut("Success") {
            *value = Value::String(serde_json::to_string(value)?);
        }
        if let Some(encoded) = continuation_payload {
            completion["ContinueAt"]["payload"] = Value::String(encoded);
        }
        Ok(serde_json::from_value(
            self.operation(
                "finish",
                json!({"id":id,"token":token,"completion":completion,
            "retention":self.config.retained_terminal}),
            )
            .await?,
        )?)
    }
    async fn register_schedule(&self, schedule: Schedule) -> Result<()> {
        schedule.validate()?;
        let signature = schedule.definition_signature();
        let mut value = serde_json::to_value(&schedule)?;
        value["job"] = wire_job(&schedule.job)?;
        self.operation(
            "register",
            json!({"schedule":value,"signature":signature,"capacity":self.config.capacity}),
        )
        .await?;
        Ok(())
    }
    async fn due_schedules(
        &self,
        kinds: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<ScheduleRecord>> {
        let mut value = self
            .operation(
                "due",
                json!({"kinds":kinds,"limit":limit,"capacity":self.config.capacity}),
            )
            .await?;
        for schedule in value
            .as_array_mut()
            .ok_or_else(|| Error::Provider("invalid schedules response".into()))?
        {
            decode_payload(&mut schedule["schedule"]["job"]["payload"])?;
        }
        Ok(serde_json::from_value(value)?)
    }
    async fn advance_schedule(
        &self,
        id: &str,
        revision: u64,
        job: Option<Job>,
        next_run_at_ms: Option<Timestamp>,
    ) -> Result<bool> {
        if let Some(job) = &job {
            job.validate()?;
        }
        let job = job.as_ref().map(wire_job).transpose()?;
        Ok(serde_json::from_value(self.operation("advance", json!({"id":id,"revision":revision,"job":job,
            "next":next_run_at_ms,"capacity":self.config.capacity,"retention":self.config.retained_terminal})).await?)?)
    }
}

// Lua cjson cannot preserve empty arrays versus objects. Payload/result JSON is
// stored as an opaque JSON string so every user value survives a round trip.
fn wire_job(job: &Job) -> Result<Value> {
    let mut value = serde_json::to_value(job)?;
    value["payload"] = Value::String(serde_json::to_string(&job.payload)?);
    Ok(value)
}

fn decode_payload(value: &mut Value) -> Result<()> {
    let encoded = value
        .as_str()
        .ok_or_else(|| Error::Provider("invalid stored payload".into()))?;
    *value = serde_json::from_str(encoded)?;
    Ok(())
}

fn decode_record(value: &mut Value) -> Result<()> {
    decode_payload(&mut value["job"]["payload"])?;
    if !value["result"].is_null() {
        decode_payload(&mut value["result"])?;
    }
    Ok(())
}

pub(crate) fn validate_lease(lease_ms: u64) -> Result<()> {
    if lease_ms == 0 || lease_ms > 9_007_199_254_740_991 {
        return Err(Error::Invalid("invalid lease duration".into()));
    }
    Ok(())
}
