///|
pub(all) struct Retire {
  partition : Int
  epoch : Int
  offset : Int64
  certificate : @causal.CausalRetirement
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn Retire::from_payload(text : String) -> Retire raise MessageError {
  let value : Retire = decode_payload(text)
  if value.partition < 0 || value.epoch < 0 || value.offset < -1L {
    raise MessageError::Malformed
  }
  let certificate = @causal.CausalRetirement::new(
    value.certificate.checkpoint_id(),
    value.certificate.ranges(),
  ) catch {
    _ => raise MessageError::Malformed
  }
  { ..value, certificate, }
}

///|
pub fn Retire::to_payload(self : Retire) -> String {
  encode_payload(self)
}

///|
pub(all) struct RetireAck {
  partition : Int
  epoch : Int
  offset : Int64
  checkpoint_id : String
} derive(Debug, Eq, ToJson, FromJson)

///|
pub fn RetireAck::from_payload(text : String) -> RetireAck raise MessageError {
  let value : RetireAck = decode_payload(text)
  if value.partition < 0 ||
    value.epoch < 0 ||
    value.offset < -1L ||
    value.checkpoint_id.is_empty() {
    raise MessageError::Malformed
  }
  value
}

///|
pub fn RetireAck::to_payload(self : RetireAck) -> String {
  encode_payload(self)
}
