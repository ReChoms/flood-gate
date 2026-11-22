use bytes::Bytes;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::path::Path;

#[derive(Debug, PartialEq)]
pub struct CompiledGrpcPayloads {
    pub rpc_path: String,
    pub payloads: Vec<Bytes>,
}

fn parse_call_target(call_target: &str) -> Result<(&str, &str), String> {
    call_target
        .trim_start_matches('/')
        .split_once('/')
        .ok_or_else(|| format!("invalid call '{call_target}'; expected Service/Method"))
}

fn load_descriptor_pool(proto_path: &Path) -> Result<DescriptorPool, String> {
    let include_dir = proto_path.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let file_descriptors = protox::compile([proto_path], [include_dir]).map_err(|err| err.to_string())?;
    DescriptorPool::from_file_descriptor_set(file_descriptors).map_err(|err| err.to_string())
}

fn find_method_input(pool: &DescriptorPool, service_name: &str, method_name: &str) -> Result<MessageDescriptor, String> {
    let service = pool.get_service_by_name(service_name)
        .ok_or_else(|| format!("service '{service_name}' not found"))?;
    let method = service.methods().find(|method| method.name() == method_name)
        .ok_or_else(|| format!("method '{method_name}' not found"))?;
    Ok(method.input())
}

fn encode_json_payload(descriptor: &MessageDescriptor, json_payload: &str) -> Result<Bytes, String> {
    let mut deserializer = serde_json::Deserializer::from_str(json_payload);
    let message = DynamicMessage::deserialize(descriptor.clone(), &mut deserializer)
        .map_err(|err| err.to_string())?;
    deserializer.end().map_err(|err| err.to_string())?;
    Ok(Bytes::from(message.encode_to_vec()))
}

pub fn compile_grpc_payloads(
    proto_path: &Path,
    call_target: &str,
    json_payloads: &[&str],
) -> Result<CompiledGrpcPayloads, String> {
    if json_payloads.is_empty() {
        return Err("at least one JSON payload is required".to_string());
    }
    let (service_name, method_name) = parse_call_target(call_target)?;
    let pool = load_descriptor_pool(proto_path)?;
    let input_descriptor = find_method_input(&pool, service_name, method_name)?;
    let mut payloads = Vec::with_capacity(json_payloads.len());
    for json_payload in json_payloads {
        payloads.push(encode_json_payload(&input_descriptor, json_payload)?);
    }
    Ok(CompiledGrpcPayloads {
        rpc_path: format!("/{service_name}/{method_name}"),
        payloads,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile_grpc_payloads_encodes_ring() {
        let temp_dir = std::env::temp_dir();
        let proto_path = temp_dir.join("floodgate_test_echo.proto");
        std::fs::write(
            &proto_path,
            "syntax = \"proto3\"; package demo; service Echo { rpc Say (Msg) returns (Msg); } message Msg { string text = 1; }",
        )
        .unwrap();
        let inputs = [r#"{"text":"hi"}"#, r#"{"text":"yo"}"#];
        let compiled = compile_grpc_payloads(&proto_path, "demo.Echo/Say", &inputs).unwrap();
        let _ = std::fs::remove_file(&proto_path);
        assert_eq!(compiled.rpc_path, "/demo.Echo/Say");
        assert_eq!(
            compiled.payloads,
            vec![Bytes::from_static(b"\x0a\x02hi"), Bytes::from_static(b"\x0a\x02yo")]
        );
    }
}
