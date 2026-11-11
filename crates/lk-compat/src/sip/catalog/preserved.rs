//! Record-local wire ownership; never a process-global raw protobuf cache.
use super::*;
use crate::sip::wire::{PreservedMessage, Stored};
impl SipCatalog {
    async fn load_record<T: Message + prost::Name + Default>(
        &self,
        key: &str,
        id: &str,
    ) -> Result<Option<Stored<T>>, CatalogError> {
        let bytes: Option<Vec<u8>> = self.query(redis::cmd("HGET").arg(key).arg(id)).await?;
        bytes.map(|bytes| record(&bytes)).transpose()
    }
    async fn page_record<T: Message + prost::Name + Default>(
        &self,
        key: &str,
        page: Option<&Pagination>,
    ) -> Result<Vec<Stored<T>>, CatalogError> {
        let bytes: Vec<Vec<u8>> = if let Some(page) = page {
            let mut ids: Vec<String> = self.query(redis::cmd("HKEYS").arg(key)).await?;
            ids.sort();
            let start = if page.after_id.is_empty() {
                0
            } else {
                ids.partition_point(|id| id <= &page.after_id)
            };
            let limit = if page.limit > 0 {
                page.limit as usize
            } else {
                1000
            };
            let ids: Vec<_> = ids.into_iter().skip(start).take(limit).collect();
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let values: Vec<Option<Vec<u8>>> =
                self.query(redis::cmd("HMGET").arg(key).arg(ids)).await?;
            values.into_iter().flatten().collect()
        } else {
            self.query(redis::cmd("HVALS").arg(key)).await?
        };
        bytes.into_iter().map(|bytes| record(&bytes)).collect()
    }
    async fn store_record<T>(
        &self,
        key: &str,
        id: &str,
        value: &Stored<T>,
    ) -> Result<(), CatalogError> {
        if id.is_empty() {
            return Err(CatalogError::EmptyId);
        }
        let _: usize = self
            .query(
                redis::cmd("HSET")
                    .arg(key)
                    .arg(id)
                    .arg(value.wire.encode_to_vec()),
            )
            .await?;
        Ok(())
    }
    pub async fn store_inbound_preserved(
        &self,
        v: &Stored<SipInboundTrunkInfo>,
    ) -> Result<(), CatalogError> {
        self.store_record(INBOUND, &v.sip_trunk_id, v).await
    }
    pub async fn store_outbound_preserved(
        &self,
        v: &Stored<SipOutboundTrunkInfo>,
    ) -> Result<(), CatalogError> {
        self.store_record(OUTBOUND, &v.sip_trunk_id, v).await
    }
    pub async fn store_rule_preserved(
        &self,
        v: &Stored<SipDispatchRuleInfo>,
    ) -> Result<(), CatalogError> {
        self.store_record(RULE, &v.sip_dispatch_rule_id, v).await
    }
    pub async fn store_legacy_preserved(
        &self,
        v: &Stored<SipTrunkInfo>,
    ) -> Result<(), CatalogError> {
        self.store_record(LEGACY, &v.sip_trunk_id, v).await
    }
    pub async fn load_legacy_preserved(
        &self,
        id: &str,
    ) -> Result<Stored<SipTrunkInfo>, CatalogError> {
        if let Some(v) = self.load_record(LEGACY, id).await? {
            return Ok(v);
        }
        if let Some(v) = self.load_record::<SipInboundTrunkInfo>(INBOUND, id).await? {
            return Ok(Stored::typed(inbound_as_legacy(&v)));
        }
        if let Some(v) = self
            .load_record::<SipOutboundTrunkInfo>(OUTBOUND, id)
            .await?
        {
            return Ok(Stored::typed(outbound_as_legacy(&v)));
        }
        Err(CatalogError::TrunkNotFound)
    }
    pub async fn load_inbound_preserved(
        &self,
        id: &str,
    ) -> Result<Option<Stored<SipInboundTrunkInfo>>, CatalogError> {
        if let Some(v) = self.load_record(INBOUND, id).await? {
            return Ok(Some(v));
        }
        let v = self
            .load_record::<SipTrunkInfo>(LEGACY, id)
            .await?
            .ok_or(CatalogError::TrunkNotFound)?;
        Ok(legacy_as_inbound(&v).map(Stored::typed))
    }
    pub async fn load_outbound_preserved(
        &self,
        id: &str,
    ) -> Result<Option<Stored<SipOutboundTrunkInfo>>, CatalogError> {
        if let Some(v) = self.load_record(OUTBOUND, id).await? {
            return Ok(Some(v));
        }
        let v = self
            .load_record::<SipTrunkInfo>(LEGACY, id)
            .await?
            .ok_or(CatalogError::TrunkNotFound)?;
        Ok(legacy_as_outbound(&v).map(Stored::typed))
    }
    pub async fn load_rule_preserved(
        &self,
        id: &str,
    ) -> Result<Stored<SipDispatchRuleInfo>, CatalogError> {
        self.load_record(RULE, id)
            .await?
            .ok_or(CatalogError::RuleNotFound)
    }
    pub async fn list_legacy_preserved(
        &self,
        req: &ListSipTrunkRequest,
    ) -> Result<Vec<Stored<SipTrunkInfo>>, CatalogError> {
        let p = req.page.as_ref();
        let mut values: Vec<Stored<SipTrunkInfo>> = self.page_record(LEGACY, p).await?;
        values.extend(
            self.page_record::<SipInboundTrunkInfo>(INBOUND, p)
                .await?
                .iter()
                .map(|v| Stored::typed(inbound_as_legacy(v))),
        );
        values.extend(
            self.page_record::<SipOutboundTrunkInfo>(OUTBOUND, p)
                .await?
                .iter()
                .map(|v| Stored::typed(outbound_as_legacy(v))),
        );
        values.retain(|v| after(p, &v.sip_trunk_id));
        go_sort::sort_by(&mut values, |a, b| a.sip_trunk_id.cmp(&b.sip_trunk_id));
        truncate(&mut values, p);
        Ok(values)
    }
    pub async fn list_inbound_preserved(
        &self,
        req: &ListSipInboundTrunkRequest,
    ) -> Result<Vec<Option<Stored<SipInboundTrunkInfo>>>, CatalogError> {
        let p = req.page.as_ref();
        let mut values: Vec<_> = self
            .page_record::<SipInboundTrunkInfo>(INBOUND, p)
            .await?
            .into_iter()
            .map(Some)
            .collect();
        values.extend(
            self.page_record::<SipTrunkInfo>(LEGACY, p)
                .await?
                .iter()
                .map(|v| legacy_as_inbound(v).map(Stored::typed)),
        );
        values.retain(|v| {
            v.as_ref()
                .is_none_or(|v| inbound_filter(req, v) && after(p, &v.sip_trunk_id))
        });
        go_sort::sort_by(&mut values, |a, b| {
            optional_trunk_id(a.as_ref().map(|v| v.sip_trunk_id.as_str())).cmp(optional_trunk_id(
                b.as_ref().map(|v| v.sip_trunk_id.as_str()),
            ))
        });
        truncate(&mut values, p);
        Ok(values)
    }
    pub async fn list_outbound_preserved(
        &self,
        req: &ListSipOutboundTrunkRequest,
    ) -> Result<Vec<Option<Stored<SipOutboundTrunkInfo>>>, CatalogError> {
        let p = req.page.as_ref();
        let mut values: Vec<_> = self
            .page_record::<SipOutboundTrunkInfo>(OUTBOUND, p)
            .await?
            .into_iter()
            .map(Some)
            .collect();
        values.extend(
            self.page_record::<SipTrunkInfo>(LEGACY, p)
                .await?
                .iter()
                .map(|v| legacy_as_outbound(v).map(Stored::typed)),
        );
        values.retain(|v| {
            v.as_ref()
                .is_none_or(|v| outbound_filter(req, v) && after(p, &v.sip_trunk_id))
        });
        go_sort::sort_by(&mut values, |a, b| {
            optional_trunk_id(a.as_ref().map(|v| v.sip_trunk_id.as_str())).cmp(optional_trunk_id(
                b.as_ref().map(|v| v.sip_trunk_id.as_str()),
            ))
        });
        truncate(&mut values, p);
        Ok(values)
    }
    pub async fn list_rules_preserved(
        &self,
        req: &ListSipDispatchRuleRequest,
    ) -> Result<Vec<Stored<SipDispatchRuleInfo>>, CatalogError> {
        let p = req.page.as_ref();
        let mut values: Vec<Stored<SipDispatchRuleInfo>> = self.page_record(RULE, p).await?;
        values.retain(|v| rule_filter(req, v) && after(p, &v.sip_dispatch_rule_id));
        go_sort::sort_by(&mut values, |a, b| {
            a.sip_dispatch_rule_id.cmp(&b.sip_dispatch_rule_id)
        });
        truncate(&mut values, p);
        Ok(values)
    }
}
fn record<T: Message + prost::Name + Default>(bytes: &[u8]) -> Result<Stored<T>, CatalogError> {
    let value = decode_stored(bytes)?;
    let wire =
        PreservedMessage::decode(&T::full_name(), bytes).map_err(CatalogError::Provenance)?;
    Ok(Stored { value, wire })
}
