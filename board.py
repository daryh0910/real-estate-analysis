"""
커뮤니티 게시판 모듈 — SQLite 기반 게시글/댓글/좋아요 관리.

Streamlit 부동산 대시보드의 차트 공유 게시판에서 사용한다.
"""

import contextlib
import hashlib
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ── 경로 설정 ──────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "board.db")
IMAGE_DIR = os.path.join(os.path.dirname(__file__), "board_images")


# ── DB 연결 ────────────────────────────────────────────────
@contextlib.contextmanager
def get_db():
    """SQLite 연결 context manager. row_factory=sqlite3.Row 사용."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """테이블 생성 + board_images 디렉토리 생성."""
    Path(IMAGE_DIR).mkdir(parents=True, exist_ok=True)

    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS posts (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                title         TEXT NOT NULL,
                description   TEXT,
                author        TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                salt          TEXT NOT NULL,
                tab_name      TEXT NOT NULL,
                image_path    TEXT NOT NULL,
                settings_json TEXT NOT NULL,
                likes         INTEGER DEFAULT 0,
                created_at    TEXT NOT NULL,
                updated_at    TEXT
            );

            CREATE TABLE IF NOT EXISTS comments (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id    INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
                author     TEXT NOT NULL,
                content    TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS likes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id    INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
                session_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(post_id, session_id)
            );

            CREATE TABLE IF NOT EXISTS saved_charts (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_hash    TEXT NOT NULL DEFAULT 'default',
                name          TEXT NOT NULL,
                settings_json TEXT NOT NULL,
                created_at    TEXT NOT NULL,
                updated_at    TEXT,
                share_token   TEXT UNIQUE,
                is_public     INTEGER NOT NULL DEFAULT 0,
                shared_at     TEXT,
                UNIQUE(owner_hash, name)
            );

            CREATE TABLE IF NOT EXISTS saved_conditions (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_hash TEXT NOT NULL DEFAULT 'default',
                name       TEXT NOT NULL,
                rules_json TEXT NOT NULL,
                combine    TEXT NOT NULL DEFAULT 'AND',
                created_at TEXT NOT NULL,
                updated_at TEXT,
                UNIQUE(owner_hash, name)
            );

            CREATE TABLE IF NOT EXISTS watchlists (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_hash      TEXT NOT NULL DEFAULT 'default',
                region          TEXT NOT NULL,
                conditions_json TEXT,
                alert_on        INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT,
                UNIQUE(owner_hash, region)
            );
        """)

        # 기존 board.db를 쓰는 경우 saved_charts에 공유용 컬럼을 후속 추가한다.
        existing_cols = {
            row["name"] for row in conn.execute("PRAGMA table_info(saved_charts)").fetchall()
        }
        migrations = {
            "share_token": "ALTER TABLE saved_charts ADD COLUMN share_token TEXT UNIQUE",
            "is_public": "ALTER TABLE saved_charts ADD COLUMN is_public INTEGER NOT NULL DEFAULT 0",
            "shared_at": "ALTER TABLE saved_charts ADD COLUMN shared_at TEXT",
        }
        for col, sql in migrations.items():
            if col not in existing_cols:
                try:
                    conn.execute(sql)
                except sqlite3.OperationalError:
                    # SQLite 버전에 따라 UNIQUE 컬럼 ADD가 제한될 수 있어 일반 컬럼으로 폴백한다.
                    if col == "share_token":
                        conn.execute("ALTER TABLE saved_charts ADD COLUMN share_token TEXT")
                    else:
                        raise
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_charts_share_token ON saved_charts(share_token)")


# ── 비밀번호 해싱 ──────────────────────────────────────────
def hash_password(password: str, salt: str = None) -> tuple:
    """sha256(salt + password) 해싱. salt 미지정 시 자동 생성. (hash_hex, salt_hex) 반환."""
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return h, salt


def verify_password(password: str, stored_hash: str, salt: str) -> bool:
    """저장된 해시와 비교하여 비밀번호 일치 여부 반환."""
    h, _ = hash_password(password, salt)
    return h == stored_hash


# ── 차트 이미지 저장 ───────────────────────────────────────
def save_chart_image(fig) -> str:
    """Plotly figure를 PNG로 저장. kaleido 미설치 시 빈 placeholder 생성. 상대 경로 반환."""
    filename = f"{uuid.uuid4().hex}.png"
    filepath = os.path.join(IMAGE_DIR, filename)
    relative = os.path.join("board_images", filename)

    try:
        # kaleido가 설치되어 있으면 정상 변환
        img_bytes = fig.to_image(format="png", width=1200, height=700)
        with open(filepath, "wb") as f:
            f.write(img_bytes)
    except Exception:
        # kaleido 미설치 등 실패 시 빈 파일로 placeholder 생성
        import warnings
        warnings.warn("kaleido 미설치 — placeholder 이미지를 생성합니다.")
        with open(filepath, "wb") as f:
            f.write(b"")

    return relative


# ── 게시글 CRUD ────────────────────────────────────────────
def create_post(title, description, author, password, tab_name, fig, settings: dict) -> int:
    """게시글 생성. 비밀번호 해싱, 이미지 저장, DB 삽입 후 post_id 반환."""
    pw_hash, salt = hash_password(password)
    image_path = save_chart_image(fig)
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        cur = conn.execute(
            """INSERT INTO posts
               (title, description, author, password_hash, salt,
                tab_name, image_path, settings_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (title, description, author, pw_hash, salt,
             tab_name, image_path, json.dumps(settings, ensure_ascii=False), now),
        )
        return cur.lastrowid


def get_posts(page: int = 1, per_page: int = 12) -> list:
    """페이지네이션된 게시글 목록. created_at DESC. dict 리스트 반환."""
    offset = (page - 1) * per_page
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM posts ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (per_page, offset),
        ).fetchall()
        return [dict(r) for r in rows]


def get_post(post_id: int) -> dict:
    """게시글 상세 조회. 없으면 None 반환."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
        return dict(row) if row else None


def get_post_count() -> int:
    """전체 게시글 수 반환."""
    with get_db() as conn:
        return conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]


def delete_post(post_id: int, password: str) -> bool:
    """비밀번호 확인 후 게시글 삭제 (이미지 파일 포함). 실패 시 False."""
    post = get_post(post_id)
    if post is None:
        return False

    if not verify_password(password, post["password_hash"], post["salt"]):
        return False

    # 이미지 파일 삭제
    img_full = os.path.join(os.path.dirname(__file__), post["image_path"])
    if os.path.exists(img_full):
        os.remove(img_full)

    with get_db() as conn:
        conn.execute("DELETE FROM posts WHERE id = ?", (post_id,))
    return True


# ── 좋아요 ─────────────────────────────────────────────────
def toggle_like(post_id: int, session_id: str) -> int:
    """좋아요 토글. 이미 존재하면 삭제, 없으면 추가. 갱신된 좋아요 수 반환."""
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM likes WHERE post_id = ? AND session_id = ?",
            (post_id, session_id),
        ).fetchone()

        if existing:
            conn.execute("DELETE FROM likes WHERE id = ?", (existing["id"],))
        else:
            conn.execute(
                "INSERT INTO likes (post_id, session_id, created_at) VALUES (?, ?, ?)",
                (post_id, session_id, now),
            )

        # posts.likes 카운트 갱신
        count = conn.execute(
            "SELECT COUNT(*) FROM likes WHERE post_id = ?", (post_id,)
        ).fetchone()[0]
        conn.execute("UPDATE posts SET likes = ? WHERE id = ?", (count, post_id))

    return count


# ── 댓글 ───────────────────────────────────────────────────
def add_comment(post_id: int, author: str, content: str) -> int:
    """댓글 추가. comment_id 반환."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        cur = conn.execute(
            "INSERT INTO comments (post_id, author, content, created_at) VALUES (?, ?, ?, ?)",
            (post_id, author, content, now),
        )
        return cur.lastrowid


def get_comments(post_id: int) -> list:
    """게시글의 댓글 목록. created_at ASC. dict 리스트 반환."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM comments WHERE post_id = ? ORDER BY created_at ASC",
            (post_id,),
        ).fetchall()
        return [dict(r) for r in rows]


# ── 개인 저장소: 차트 / 조건 / 관심지역 ─────────────────────────────────
DEFAULT_OWNER = "default"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_chart_settings(name: str, settings: dict, owner_hash: str = DEFAULT_OWNER) -> int:
    """자유차트 설정 저장. 같은 이름은 덮어쓴다."""
    now = _utc_now()
    payload = json.dumps(settings or {}, ensure_ascii=False)
    with get_db() as conn:
        conn.execute(
            """INSERT INTO saved_charts (owner_hash, name, settings_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(owner_hash, name)
               DO UPDATE SET settings_json = excluded.settings_json, updated_at = excluded.updated_at""",
            (owner_hash, name, payload, now, now),
        )
        row = conn.execute(
            "SELECT id FROM saved_charts WHERE owner_hash = ? AND name = ?",
            (owner_hash, name),
        ).fetchone()
        return int(row["id"])


def list_saved_charts(owner_hash: str = DEFAULT_OWNER) -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM saved_charts WHERE owner_hash = ? ORDER BY updated_at DESC, created_at DESC",
            (owner_hash,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_saved_chart(chart_id: int, owner_hash: str = DEFAULT_OWNER) -> dict:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM saved_charts WHERE id = ? AND owner_hash = ?",
            (chart_id, owner_hash),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["settings"] = json.loads(result.get("settings_json") or "{}")
        return result


def share_saved_chart(chart_id: int, owner_hash: str = DEFAULT_OWNER) -> str:
    """저장 차트를 공개 공유 상태로 바꾸고 안정적인 토큰을 반환한다."""
    now = _utc_now()
    with get_db() as conn:
        row = conn.execute(
            "SELECT id, share_token FROM saved_charts WHERE id = ? AND owner_hash = ?",
            (chart_id, owner_hash),
        ).fetchone()
        if not row:
            return None
        token = row["share_token"] or secrets.token_urlsafe(16)
        conn.execute(
            """UPDATE saved_charts
               SET share_token = ?, is_public = 1, shared_at = COALESCE(shared_at, ?), updated_at = ?
               WHERE id = ? AND owner_hash = ?""",
            (token, now, now, chart_id, owner_hash),
        )
        return token


def get_shared_chart(share_token: str) -> dict:
    """공개 공유 토큰으로 저장 차트를 조회한다. 비공개/없음이면 None."""
    if not share_token:
        return None
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM saved_charts WHERE share_token = ? AND is_public = 1",
            (share_token,),
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["settings"] = json.loads(result.get("settings_json") or "{}")
        return result


def revoke_shared_chart(chart_id: int, owner_hash: str = DEFAULT_OWNER) -> bool:
    """저장 차트의 공개 공유를 해제한다. 토큰은 재사용을 막기 위해 삭제한다."""
    with get_db() as conn:
        cur = conn.execute(
            """UPDATE saved_charts
               SET share_token = NULL, is_public = 0, shared_at = NULL, updated_at = ?
               WHERE id = ? AND owner_hash = ?""",
            (_utc_now(), chart_id, owner_hash),
        )
        return cur.rowcount > 0


def delete_saved_chart(chart_id: int, owner_hash: str = DEFAULT_OWNER) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM saved_charts WHERE id = ? AND owner_hash = ?",
            (chart_id, owner_hash),
        )
        return cur.rowcount > 0


def save_condition_set(name: str, rules: list, combine: str = "AND", owner_hash: str = DEFAULT_OWNER) -> int:
    """조건 빌더 규칙 저장. 같은 이름은 덮어쓴다."""
    now = _utc_now()
    payload = json.dumps(rules or [], ensure_ascii=False)
    with get_db() as conn:
        conn.execute(
            """INSERT INTO saved_conditions (owner_hash, name, rules_json, combine, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(owner_hash, name)
               DO UPDATE SET rules_json = excluded.rules_json, combine = excluded.combine, updated_at = excluded.updated_at""",
            (owner_hash, name, payload, combine, now, now),
        )
        row = conn.execute(
            "SELECT id FROM saved_conditions WHERE owner_hash = ? AND name = ?",
            (owner_hash, name),
        ).fetchone()
        return int(row["id"])


def list_saved_conditions(owner_hash: str = DEFAULT_OWNER) -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM saved_conditions WHERE owner_hash = ? ORDER BY updated_at DESC, created_at DESC",
            (owner_hash,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["rules"] = json.loads(item.get("rules_json") or "[]")
            result.append(item)
        return result


def upsert_watchlist(region: str, conditions: list = None, alert_on: bool = False, owner_hash: str = DEFAULT_OWNER) -> int:
    """관심지역 등록. 같은 지역은 조건/알림 설정을 갱신한다."""
    now = _utc_now()
    payload = json.dumps(conditions or [], ensure_ascii=False)
    with get_db() as conn:
        conn.execute(
            """INSERT INTO watchlists (owner_hash, region, conditions_json, alert_on, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(owner_hash, region)
               DO UPDATE SET conditions_json = excluded.conditions_json,
                             alert_on = excluded.alert_on,
                             updated_at = excluded.updated_at""",
            (owner_hash, region, payload, int(bool(alert_on)), now, now),
        )
        row = conn.execute(
            "SELECT id FROM watchlists WHERE owner_hash = ? AND region = ?",
            (owner_hash, region),
        ).fetchone()
        return int(row["id"])


def list_watchlists(owner_hash: str = DEFAULT_OWNER) -> list:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM watchlists WHERE owner_hash = ? ORDER BY updated_at DESC, created_at DESC",
            (owner_hash,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["conditions"] = json.loads(item.get("conditions_json") or "[]")
            result.append(item)
        return result


def delete_watchlist(watchlist_id: int, owner_hash: str = DEFAULT_OWNER) -> bool:
    with get_db() as conn:
        cur = conn.execute(
            "DELETE FROM watchlists WHERE id = ? AND owner_hash = ?",
            (watchlist_id, owner_hash),
        )
        return cur.rowcount > 0


# ── 설정 캡처 ──────────────────────────────────────────────
def capture_current_settings() -> dict:
    """st.session_state에서 글로벌 필터 + 탭별 설정을 추출하여 dict 반환.

    존재하는 키만 포함하고, 없는 키는 건너뛴다.
    """
    import streamlit as st

    result = {}

    # 글로벌 필터 키
    global_keys = ["analysis_mode", "selected_sido", "selected_codes",
                   "selected_years", "freq"]

    # 탭 4 (수식 계산기) 키
    tab4_keys = []
    for i in range(4):
        tab4_keys.extend([f"f5_ta_{i}", f"f5_label_{i}", f"f5_unit_{i}", f"f5_enabled_{i}"])
    tab4_keys.extend(["calc5_mode", "calc5_base", "calc5_sido", "calc5_y1", "calc5_y2"])

    # 탭 2 (시계열 비교) 키
    tab2_keys = ["left", "right", "ts_sido", "norm_vars", "price_cmp_var", "gap_sido"]

    all_keys = global_keys + tab4_keys + tab2_keys

    for key in all_keys:
        if key in st.session_state:
            result[key] = st.session_state[key]

    return result


# ── 모듈 임포트 시 DB 초기화 ───────────────────────────────
init_db()
