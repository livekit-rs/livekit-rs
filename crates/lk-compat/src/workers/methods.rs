// Draft transfer only. Fixed generated Go descriptors: see SOURCE.json.
use crate::service_bus::rpc::MethodSpec;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerMethod {
    pub service: &'static str,
    pub method: &'static str,
    pub affinity: bool,
    pub multi: bool,
    pub require_claim: bool,
    pub queue: bool,
}
impl WorkerMethod {
    pub fn spec(self, topics: &[&str]) -> MethodSpec {
        MethodSpec {
            service: self.service.into(),
            method: self.method.into(),
            topics: topics.iter().map(|s| (*s).into()).collect(),
            require_claim: self.require_claim,
            queue: self.queue,
        }
    }
    // A multi method must NOT be fed to unary and silently keep the first answer.
    pub fn is_unary(self) -> bool {
        !self.multi
    }
}
pub const METHODS: &[WorkerMethod] = &[
    WorkerMethod {
        service: "IOInfo",
        method: "CreateEgress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "UpdateEgress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "GetEgress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "ListEgress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "UpdateMetrics",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "CreateIngress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "GetIngressInfo",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "UpdateIngressState",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "GetSIPTrunkAuthentication",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "EvaluateSIPDispatchRules",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "UpdateSIPCallState",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfo",
        method: "RecordCallContext",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfoSIP",
        method: "GetSIPTrunkAuthentication",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfoSIP",
        method: "EvaluateSIPDispatchRules",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfoSIP",
        method: "UpdateSIPCallState",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IOInfoSIP",
        method: "RecordCallContext",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "EgressInternal",
        method: "StartEgress",
        affinity: true,
        multi: false,
        require_claim: true,
        queue: false,
    },
    WorkerMethod {
        service: "EgressInternal",
        method: "ListActiveEgress",
        affinity: false,
        multi: true,
        require_claim: false,
        queue: false,
    },
    WorkerMethod {
        service: "EgressHandler",
        method: "StopEgress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "EgressHandler",
        method: "UpdateStream",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "ReplayExport",
        method: "EgressReady",
        affinity: false,
        multi: false,
        require_claim: false,
        queue: true,
    },
    WorkerMethod {
        service: "IngressInternal",
        method: "StartIngress",
        affinity: true,
        multi: false,
        require_claim: true,
        queue: false,
    },
    WorkerMethod {
        service: "IngressInternal",
        method: "ListActiveIngress",
        affinity: false,
        multi: true,
        require_claim: false,
        queue: false,
    },
    WorkerMethod {
        service: "IngressHandler",
        method: "UpdateIngress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IngressHandler",
        method: "DeleteIngress",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IngressHandler",
        method: "DeleteWHIPResource",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IngressHandler",
        method: "ICERestartWHIPResource",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "IngressHandler",
        method: "WHIPRTCConnectionNotify",
        affinity: false,
        multi: false,
        require_claim: false,
        queue: true,
    },
    WorkerMethod {
        service: "SIPInternal",
        method: "CreateSIPParticipant",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
    WorkerMethod {
        service: "SIPInternal",
        method: "TransferSIPParticipant",
        affinity: false,
        multi: false,
        require_claim: true,
        queue: true,
    },
];
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_service_identity_and_transport_flags_are_not_conflated() {
        let start = METHODS
            .iter()
            .find(|m| m.service == "EgressInternal" && m.method == "StartEgress")
            .unwrap();
        assert!(start.affinity && start.require_claim && !start.queue && start.is_unary());
        let list = METHODS
            .iter()
            .find(|m| m.service == "EgressInternal" && m.method == "ListActiveEgress")
            .unwrap();
        assert!(list.multi && !list.require_claim && !list.is_unary());
        let stop = METHODS
            .iter()
            .find(|m| m.service == "EgressHandler" && m.method == "StopEgress")
            .unwrap();
        assert!(stop.queue && stop.require_claim);
        assert_eq!(
            stop.spec(&["EG_public_fixture"]).topics,
            vec!["EG_public_fixture"]
        );
    }
}
