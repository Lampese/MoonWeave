///|
/// User-facing remote lifecycle handle. The DriverClient remains an internal
/// transport adapter; callers interact with the same lifecycle vocabulary as
/// LocalJobHandle.
pub struct RemoteJobHandle {
  submission : JobSubmission
  client : DriverClient
  mut last_status : JobRunStatus
}

///|
pub async fn JobSubmission::run_remote(
  self : JobSubmission,
  config : DriverConfig,
) -> RemoteJobHandle raise ApiError {
  let client = DriverClient::connect(config)
  let status = client.submit(self)
  { submission: self, client, last_status: status, }
}

///|
pub fn RemoteJobHandle::submission(self : RemoteJobHandle) -> JobSubmission {
  self.submission
}

///|
pub fn RemoteJobHandle::status(self : RemoteJobHandle) -> JobRunStatus {
  self.last_status
}

///|
pub async fn RemoteJobHandle::refresh(
  self : RemoteJobHandle,
) -> JobRunStatus raise ApiError {
  self.last_status = self.client.status()
  self.last_status
}

///|
pub async fn RemoteJobHandle::input(
  self : RemoteJobHandle,
  events : ArrayView[Event],
) -> JobRunStatus raise ApiError {
  self.last_status = self.client.input(events)
  self.last_status
}

///|
pub async fn RemoteJobHandle::drain(
  self : RemoteJobHandle,
) -> JobRunStatus raise ApiError {
  self.last_status = self.client.drain()
  self.last_status
}

///|
pub async fn RemoteJobHandle::cancel(
  self : RemoteJobHandle,
) -> JobRunStatus raise ApiError {
  self.last_status = self.client.cancel()
  self.last_status
}

///|
pub async fn RemoteJobHandle::await_completion(
  self : RemoteJobHandle,
  poll_interval_ms? : Int = 100,
  timeout_ms? : Int = 30000,
) -> JobRunStatus raise ApiError {
  self.last_status = self.client.await_completion(
    poll_interval_ms~,
    timeout_ms~,
  )
  self.last_status
}

///|
pub fn RemoteJobHandle::close(self : RemoteJobHandle) -> Unit {
  self.client.close()
}
