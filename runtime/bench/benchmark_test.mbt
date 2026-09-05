///|
test "reachability benchmark reports deterministic coordination savings" {
  let metrics = run_reachability_benchmark(8)
  assert_eq(metrics.events(), 8)
  assert_eq(metrics.candidate_step(), 1)
  assert_eq(metrics.stable_step(), 1)
  assert_eq(metrics.barrier_rounds(), 8)
  assert_eq(metrics.targeted_probes(), 1)
  assert_eq(metrics.barrier_messages(), 16)
  assert_eq(metrics.targeted_messages(), 3)
}
