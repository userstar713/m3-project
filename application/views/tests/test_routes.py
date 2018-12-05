def test_execute_pipeline(monkeypatch, client):

    def mock_sync():
        pass

    monkeypatch.setattr(
        'application.tasks.synchronization.execute_pipeline_task',
        mock_sync)
    resp = client.get('/api/1/source/1/execute_pipeline/')
    assert resp.status_code == 200
    assert resp.json == {'data': {'status': 'ok'}}
