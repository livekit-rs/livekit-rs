use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use prost::Message;
use serde_json::Value;

fn parse<T: Message + prost::Name + Default>(v: &Value) -> T {
    lk_protocol::json::decode(&serde_json::to_vec(v).unwrap()).unwrap()
}
fn parse_items<T: Message + prost::Name + Default>(v: &Value) -> Vec<T> {
    v.as_array().unwrap().iter().map(parse).collect()
}

fn outcome(v: ValidationResult) -> (Option<String>, Vec<String>) {
    (v.hard, v.soft)
}
fn expected_message<T: Message + Default + PartialEq + std::fmt::Debug>(
    actual: &T,
    expected: &Value,
    name: &str,
) {
    let bytes = STANDARD
        .decode(expected["bytes"].as_str().unwrap_or_default())
        .unwrap();
    assert_eq!(
        *actual,
        T::decode(bytes.as_slice()).unwrap(),
        "{name}: message"
    );
}
#[test]
fn pinned_go_sip_domain_vectors() {
    let vectors: Vec<Value> = serde_json::from_str(include_str!("oracle-vectors.json")).unwrap();
    let results: Vec<Value> = serde_json::from_str(include_str!("oracle-results.json")).unwrap();
    assert_eq!(vectors.len(), results.len());
    for (v, expected) in vectors.iter().zip(&results) {
        let name = v["name"].as_str().unwrap();
        assert_eq!(expected["name"].as_str(), Some(name));
        let op = v["operation"].as_str().unwrap();
        let request = &v["request"];
        let mut hard = None;
        let mut soft = vec![];
        let mut check_mutation: Option<Box<dyn FnOnce() + '_>> = None;
        match op {
            "create_inbound" => (hard, soft) = outcome(validate_create_inbound(&parse(request))),
            "create_outbound" => (hard, soft) = outcome(validate_create_outbound(&parse(request))),
            "apply_inbound" => match apply_inbound_request(&parse(request), &parse(&v["existing"]))
            {
                Ok((got, result)) => {
                    (hard, soft) = outcome(result);
                    check_mutation = Some(Box::new(move || expected_message(&got, expected, name)))
                }
                Err(e) => hard = Some(e.message),
            },
            "apply_outbound" => {
                match apply_outbound_request(&parse(request), &parse(&v["existing"])) {
                    Ok((got, result)) => {
                        (hard, soft) = outcome(result);
                        check_mutation =
                            Some(Box::new(move || expected_message(&got, expected, name)))
                    }
                    Err(e) => hard = Some(e.message),
                }
            }
            "create_dispatch" => {
                let req = parse(request);
                match validate_create_dispatch(&req) {
                    Ok(()) => {
                        let got = dispatch_info(&req);
                        check_mutation =
                            Some(Box::new(move || expected_message(&got, expected, name)));
                    }
                    Err(e) => hard = Some(e),
                }
            }
            "apply_dispatch" => {
                match apply_dispatch_request(&parse(request), &parse(&v["existing"])) {
                    Ok(got) => {
                        check_mutation =
                            Some(Box::new(move || expected_message(&got, expected, name)))
                    }
                    Err(e) => hard = Some(e.message),
                }
            }
            "list" => {
                let mut info: pb::SipInboundTrunkInfo = parse(&v["existing"]);
                match apply_list_update(&mut info.numbers, Some(&parse(request))) {
                    Ok(()) => {
                        check_mutation =
                            Some(Box::new(move || expected_message(&info, expected, name)))
                    }
                    Err(e) => hard = Some(e),
                }
            }
            "upgrade_inbound" => {
                let mut info = parse(request);
                upgrade_inbound(&mut info);
                check_mutation = Some(Box::new(move || expected_message(&info, expected, name)))
            }
            "upgrade_outbound" => {
                let mut info = parse(request);
                upgrade_outbound(&mut info);
                check_mutation = Some(Box::new(move || expected_message(&info, expected, name)))
            }
            "upgrade_dispatch" => {
                let mut info = parse(request);
                upgrade_dispatch(&mut info);
                check_mutation = Some(Box::new(move || expected_message(&info, expected, name)))
            }
            "normalize" => assert_eq!(
                normalize_number(v["value"].as_str().unwrap()),
                expected["text"].as_str().unwrap(),
                "{name}"
            ),
            "inbound_conflict" => {
                hard =
                    validate_inbound_conflicts(&parse_items::<pb::SipInboundTrunkInfo>(&v["items"]))
                        .err()
            }
            "dispatch_conflict" => {
                hard = validate_dispatch_conflicts(&parse_items::<pb::SipDispatchRuleInfo>(
                    &v["items"],
                ))
                .err()
            }
            _ => panic!("unknown oracle operation {op}"),
        }
        assert_eq!(hard.as_deref(), expected["hard"].as_str(), "{name}: hard");
        let expected_soft: Vec<String> = serde_json::from_value(expected["soft"].clone()).unwrap();
        assert_eq!(soft, expected_soft, "{name}: soft");
        if hard.is_none()
            && let Some(check) = check_mutation
        {
            check();
        }
    }
}
