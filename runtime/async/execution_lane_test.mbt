///|
async test "execution slots serialize sibling shards without blocking another slot" {
  let first = ExecutionLane::new(0)
  let second = ExecutionLane::new(1)
  let occupied = first.acquire()
  @async_runtime.with_task_group <| group => {
    let waiting = group.spawn() <| () => { first.acquire() }
    let independent = group.spawn() <| () => { second.acquire() }
    independent.wait().release()
    assert_true(waiting.try_wait() is None)
    assert_eq(first.admissions(), 1L)
    occupied.release()
    occupied.release()
    waiting.wait().release()
    assert_eq(first.admissions(), 2L)
  }
}
