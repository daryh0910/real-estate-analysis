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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
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
        """)


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
