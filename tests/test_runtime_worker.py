import importlib


def _load_database(tmp_path, monkeypatch):
    import database

    db_file = tmp_path / "worker_runtime_test.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    importlib.reload(database)
    monkeypatch.setattr(database, "DB_PATH", str(db_file))
    database.init_db()
    return database


def test_worker_lease_allows_only_one_owner(tmp_path, monkeypatch):
    database = _load_database(tmp_path, monkeypatch)

    assert database.acquire_worker_lease("worker-a", lease_seconds=90) is True
    assert database.acquire_worker_lease("worker-b", lease_seconds=90) is False
    assert database.heartbeat_worker_lease("worker-a", lease_seconds=90) is True

    runtime = database.get_runtime_diagnostics()
    assert runtime["worker_id"] == "worker-a"
    assert runtime["worker_online"] is True


def test_manual_check_queue_deduplicates_and_tracks_completion(tmp_path, monkeypatch):
    database = _load_database(tmp_path, monkeypatch)

    request_id, created = database.enqueue_manual_check_request(requested_by="127.0.0.1")
    assert created is True
    duplicate_id, duplicate_created = database.enqueue_manual_check_request(requested_by="127.0.0.1")
    assert duplicate_created is False
    assert duplicate_id == request_id

    claimed = database.claim_next_manual_check_request("worker-a")
    assert claimed is not None
    assert claimed["id"] == request_id

    runtime = database.get_runtime_diagnostics()
    assert runtime["running_manual_checks"] == 1

    database.complete_manual_check_request(request_id, "worker-a", "completed")
    runtime = database.get_runtime_diagnostics()
    assert runtime["running_manual_checks"] == 0
    assert runtime["queued_manual_checks"] == 0


def test_job_runs_appear_in_runtime_diagnostics(tmp_path, monkeypatch):
    database = _load_database(tmp_path, monkeypatch)

    assert database.acquire_worker_lease("worker-a", lease_seconds=90) is True
    run_id = database.begin_worker_job("worker-a", "periodic_price_check", "scheduled")
    assert run_id is not None
    database.finish_worker_job("worker-a", run_id, "completed")

    runtime = database.get_runtime_diagnostics()
    assert runtime["last_job_name"] == "periodic_price_check"
    assert runtime["last_job_status"] == "completed"
    assert runtime["last_periodic_check"]["status"] == "completed"
