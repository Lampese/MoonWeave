///|
async fn DriverStore::begin_rescale(
  self : DriverStore,
  workers : Array[String],
  parallelism? : Int,
) -> Unit raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock rescale intent", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.phase != Running {
    raise ApiError::InvalidJobPhase(expected=Running, actual=self.phase)
  }
  let ordered = workers.copy()
  ordered.sort()
  if parallelism is Some(value) &&
    (value <= 0 || value > self.compiled.unwrap().maximum_parallelism()) {
    raise ApiError::InvalidPartitionCount(value)
  }
  match self.rescale_workers {
    Some(existing) if existing == ordered &&
      self.rescale_parallelism == parallelism => return
    Some(_) =>
      raise ApiError::InvalidDriverCommand("another rescale is pending")
    None => ()
  }
  let entry = {
    ..driver_entry("rescale"),
    rescale_workers: Some(ordered),
    rescale_resume: Some(!self.paused),
    rescale_parallelism: parallelism,
  }
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist rescale intent", error)
    },
  )
  self.install_entry(entry)
}

///|
async fn DriverStore::complete_rescale(
  self : DriverStore,
) -> Unit raise ApiError {
  self.write_lock.acquire() catch {
    error => raise driver_error("lock rescale completion", error)
  }
  defer self.write_lock.release()
  self.check_open()
  if self.rescale_workers is None {
    raise ApiError::InvalidDriverCommand("no rescale is pending")
  }
  let entry = driver_entry("rescaled")
  ignore(
    self.journal.append(driver_entry_line(entry)) catch {
      error => raise driver_error("persist rescale completion", error)
    },
  )
  self.install_entry(entry)
}
