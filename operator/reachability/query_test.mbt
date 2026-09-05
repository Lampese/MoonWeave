///|
test "reachability query stays tentative until every obligation closes" {
  let query = Query::new("query-1", "A", "D", [2, 0])
  assert_eq(query.status(), QueryStatus::Tentative)
  assert_eq(query.obligations().map(item => item.partition()), [0, 2])
  assert_false(query.observe(Reachable::new("A", "C")))
  assert_eq(query.status(), QueryStatus::Tentative)
  assert_true(query.observe(Reachable::new("A", "D")))
  assert_eq(query.status(), QueryStatus::Tentative)
  assert_true(query.close_partition(0))
  assert_eq(query.status(), QueryStatus::Tentative)
  assert_true(query.close_partition(2))
  assert_eq(query.status(), QueryStatus::Stable)
  assert_false(query.close_partition(2))
}

///|
test "closed reachability query can prove no match" {
  let query = Query::new("query-2", "A", "D", [1, 3])
  assert_eq(query.status(), QueryStatus::Tentative)
  assert_true(query.close_partition(3))
  assert_true(query.close_partition(1))
  assert_eq(query.status(), QueryStatus::ResolvedNoMatch)
  assert_false(query.matched())
}

///|
test "duplicate partitions do not duplicate obligations" {
  let query = Query::new("query-3", "A", "B", [4, 4])
  assert_eq(query.obligations().length(), 1)
  assert_eq(query.obligations()[0].partition(), 4)
}

///|
test "query rejects invalid identity and partition inputs" {
  try Query::new("", "A", "B", []) catch {
    QueryError::EmptyQueryId => ()
    _ => fail("expected empty query id")
  } noraise {
    _ => fail("expected empty query id")
  }
  try Query::new("query-4", "", "B", []) catch {
    QueryError::EmptyNode => ()
    _ => fail("expected empty node")
  } noraise {
    _ => fail("expected empty node")
  }
  try Query::new("query-4", "A", "B", [-1]) catch {
    QueryError::InvalidPartition(partition) => assert_eq(partition, -1)
    _ => fail("expected invalid partition")
  } noraise {
    _ => fail("expected invalid partition")
  }
}

///|
test "resolved query produces a verifiable result certificate" {
  let query = Query::new("query-cert", "A", "C", [0])
  ignore(query.observe(Reachable::new("A", "C")))
  ignore(query.close_partition(0))
  let certificate = query.certificate("frontier-digest", [])
  assert_eq(certificate.status(), QueryStatus::Stable)
  assert_eq(certificate.result_digest(), "match:A->C")
  assert_true(certificate.verify(query, "frontier-digest"))
  assert_false(certificate.verify(query, "other-frontier"))
}

///|
test "tentative query cannot produce a result certificate" {
  let query = Query::new("query-cert-pending", "A", "C", [0])
  try query.certificate("frontier-digest", []) catch {
    QueryError::NotResolved => ()
    _ => fail("expected unresolved query error")
  } noraise {
    _ => fail("expected unresolved query error")
  }
}
