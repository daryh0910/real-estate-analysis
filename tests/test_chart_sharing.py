import board


def setup_temp_board(tmp_path, monkeypatch):
    monkeypatch.setattr(board, "DB_PATH", str(tmp_path / "board.db"))
    monkeypatch.setattr(board, "IMAGE_DIR", str(tmp_path / "board_images"))
    board.init_db()


def test_saved_chart_can_be_shared_and_loaded_by_token(tmp_path, monkeypatch):
    setup_temp_board(tmp_path, monkeypatch)
    chart_id = board.save_chart_settings("입주물량 비교", {"super_regions": ["서울"], "super_indicators": ["입주예정_세대수"]})

    token = board.share_saved_chart(chart_id)
    shared = board.get_shared_chart(token)

    assert isinstance(token, str)
    assert len(token) >= 16
    assert shared["id"] == chart_id
    assert shared["name"] == "입주물량 비교"
    assert shared["settings"]["super_regions"] == ["서울"]
    assert shared["is_public"] == 1


def test_share_token_is_stable_and_can_be_revoked(tmp_path, monkeypatch):
    setup_temp_board(tmp_path, monkeypatch)
    chart_id = board.save_chart_settings("전세가율", {"super_mode": "같은 기준으로 비교"})

    token1 = board.share_saved_chart(chart_id)
    token2 = board.share_saved_chart(chart_id)
    revoked = board.revoke_shared_chart(chart_id)

    assert token1 == token2
    assert revoked
    assert board.get_shared_chart(token1) is None
