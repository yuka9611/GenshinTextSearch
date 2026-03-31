from DBConfig import conn
import entitySourceImport


def _safe_count() -> int:
    try:
        row = conn.execute("SELECT COUNT(*) FROM text_source_entity").fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def main():
    before = _safe_count()
    changes_before = int(getattr(conn, "total_changes", 0))
    entitySourceImport.insertEntitySourcesDelta(commit=True, interactive=True)
    changes_after = int(getattr(conn, "total_changes", 0))
    after = _safe_count()
    inserted = max(0, changes_after - changes_before)
    print(f"inserted={inserted} rows_before={before} rows_after={after}")


if __name__ == "__main__":
    main()

