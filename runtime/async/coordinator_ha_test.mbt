///|
async fn with_coordinator_ha_paths(
  f : async (Array[CoordinatorReplicaConfig]) -> Unit,
) -> Unit {
  let root = @fs.tmpdir(prefix="moonweave-coordinator-ha")
  let paths = [
    root + "/replica-0.wal",
    root + "/replica-1.wal",
    root + "/replica-2.wal",
  ]
  let configs : Array[CoordinatorReplicaConfig] = []
  for index, path in paths {
    configs.push(
      CoordinatorReplicaConfig::new(
        "replica-" + index.to_string(),
        path,
        checkpoint_path=path + ".json",
      ),
    )
  }
  defer {
    for path in paths {
      for
        suffix in [
          "", ".lock", ".next", ".next.lock", ".repair", ".json", ".json.tmp",
        ] {
        @fs.remove(path + suffix) catch {
          _ => ()
        }
      }
    }
    @fs.rmdir(root) catch {
      _ => ()
    }
  }
  f(configs)
}

///|
async test "coordinator HA refuses writes without a majority and catches up a replica" {
  with_coordinator_ha_paths <| configs => {
    let ha = CoordinatorHa::open("ha-cluster", "coordinator", configs)
    let boot_a = @protocol.Incarnation::new("boot-a")
    let boot_b = @protocol.Incarnation::new("boot-b")
    ignore(ha.register_worker("worker-a", boot_a, ["reachability"]))
    let assignment = ha.assign(0, "worker-a")
    assert_eq(assignment.epoch(), 0)
    assert_eq(ha.available_replicas(), 3)

    ha.deactivate_replica(1)
    ignore(ha.register_worker("worker-b", boot_b, []))
    ha.deactivate_replica(2)
    try ha.assign(1, "worker-a") catch {
      CoordinatorHaError::NoQuorum(required~, available~) => {
        assert_eq(required, 2)
        assert_eq(available, 1)
      }
      _ => fail("expected quorum rejection")
    } noraise {
      _ => fail("expected quorum rejection")
    }

    ha.recover_replica(1)
    assert_eq(ha.available_replicas(), 2)
    let restored = ha.snapshot()
    assert_eq(restored.workers().length(), 2)
    assert_eq(restored.assignments().length(), 1)
    ignore(
      ha.register_worker("worker-c", @protocol.Incarnation::new("boot-c"), []),
    )
    ha.checkpoint(compact=true)
    ha.close()

    let reopened = CoordinatorHa::open("ha-cluster", "coordinator", configs)
    defer reopened.close()
    assert_eq(reopened.snapshot().workers().length(), 3)
    assert_eq(reopened.snapshot().assignments().length(), 1)
  }
}

///|
async test "coordinator HA validates replica identity and quorum configuration" {
  with_coordinator_ha_paths <| configs => {
    try CoordinatorHa::open("ha-cluster", "coordinator", configs[0:2]) catch {
      CoordinatorHaError::InvalidReplicaCount(value) => assert_eq(value, 2)
      _ => fail("expected replica count rejection")
    } noraise {
      _ => fail("expected replica count rejection")
    }
    try
      CoordinatorHa::open("ha-cluster", "coordinator", configs, quorum=1)
    catch {
      CoordinatorHaError::InvalidQuorum(value) => assert_eq(value, 1)
      _ => fail("expected quorum rejection")
    } noraise {
      _ => fail("expected quorum rejection")
    }
  }
}

///|
async test "coordinator HA elects an admitted leader after fencing the old one" {
  with_coordinator_ha_paths <| configs => {
    let ha = CoordinatorHa::open("ha-cluster", "coordinator", configs)
    assert_eq(ha.leader_index(), 0)
    assert_eq(ha.leader_id(), "replica-0")
    ha.deactivate_replica(0)
    assert_eq(ha.leader_index(), 1)
    assert_eq(ha.leader_id(), "replica-1")
    let boot = @protocol.Incarnation::new("boot-leader")
    ignore(ha.register_worker("worker-a", boot, []))
    assert_eq(ha.snapshot().workers().length(), 1)
    ha.close()
  }
}
