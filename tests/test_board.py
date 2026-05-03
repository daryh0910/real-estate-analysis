import board


def setup_temp_board(tmp_path, monkeypatch):
    monkeypatch.setattr(board, "DB_PATH", str(tmp_path / "board.db"))
    monkeypatch.setattr(board, "IMAGE_DIR", str(tmp_path / "board_images"))
    board.init_db()


def test_saved_chart_crud(tmp_path, monkeypatch):
    setup_temp_board(tmp_path, monkeypatch)

    chart_id = board.save_chart_settings("가격 전세", {"regions": ["서울"], "mode": "index"})
    charts = board.list_saved_charts()
    loaded = board.get_saved_chart(chart_id)

    assert len(charts) == 1
    assert loaded["settings"]["regions"] == ["서울"]

    board.save_chart_settings("가격 전세", {"regions": ["부산"]})
    loaded_again = board.get_saved_chart(chart_id)
    assert loaded_again["settings"]["regions"] == ["부산"]

    assert board.delete_saved_chart(chart_id)
    assert board.list_saved_charts() == []


def test_saved_conditions_and_watchlist(tmp_path, monkeypatch):
    setup_temp_board(tmp_path, monkeypatch)

    rules = [{"column": "PIR", "op": "<", "value": "15"}]
    condition_id = board.save_condition_set("저평가", rules, combine="AND")
    conditions = board.list_saved_conditions()
    watch_id = board.upsert_watchlist("서울", rules, alert_on=True)
    watchlists = board.list_watchlists()

    assert condition_id > 0
    assert conditions[0]["rules"] == rules
    assert watch_id > 0
    assert watchlists[0]["region"] == "서울"
    assert watchlists[0]["alert_on"] == 1

    assert board.delete_watchlist(watch_id)
    assert board.list_watchlists() == []
