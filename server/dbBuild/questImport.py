import os
import json
import hashlib
import re
import sys

from lightweight_progress import LightweightProgress

from DBConfig import conn, DATA_PATH
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched
from quest_hash_map_utils import (
    ensure_quest_hash_map_schema as _ensure_quest_hash_map_schema,
    refresh_quest_hash_map_for_quest_ids as _refresh_quest_hash_map_for_quest_ids,
    refresh_quest_hash_map_for_talk_ids as _refresh_quest_hash_map_for_talk_ids,
)
from quest_utils import extract_quest_id, extract_quest_row, extract_quest_talk_ids
from version_control import backfill_quest_created_version_from_textmap as _backfill_quest_created_version_from_textmap
from version_control import get_current_version, get_or_create_version_id, should_update_version


def _print_skip_summary(title: str, skipped_files: list[str], sample_size: int = 10):
    if not skipped_files:
        return
    samples = skipped_files[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(skipped_files) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SKIP] {title}: {len(skipped_files)} files skipped. samples: {sample_text}")


def _print_issue_summary(title: str, issues: list[str], sample_size: int = 10):
    if not issues:
        return
    samples = issues[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(issues) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SUMMARY] {title}: {len(issues)}. samples: {sample_text}")


def _load_json_file(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _is_hidden_quest_obj(obj: dict) -> bool:
    quest_type = None
    if isinstance(obj, dict):
        if "questType" in obj:
            quest_type = obj.get("questType")
        elif "NCDLPENPKKC" in obj:
            quest_type = obj.get("NCDLPENPKKC")
    return quest_type == "QUEST_HIDDEN"


def _ensure_quest_version_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS quest_text_signature (
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            dialogue_signature TEXT NOT NULL
        )
        """
    )
    _ensure_quest_hash_map_schema(cursor)


def _normalize_talk_ids(talk_ids):
    normalized = []
    seen = set()
    for talk_id in talk_ids:
        try:
            tid = int(talk_id)
        except Exception:
            continue
        if tid in seen:
            continue
        seen.add(tid)
        normalized.append(tid)
    return normalized


def _build_quest_dialogue_signature(cursor, talk_ids):
    talk_ids = _normalize_talk_ids(talk_ids)
    if not talk_ids:
        return ""
    placeholders = ",".join(["?"] * len(talk_ids))
    rows = cursor.execute(
        f"""
        SELECT textHash, COUNT(*)
        FROM dialogue
        WHERE talkId IN ({placeholders})
          AND textHash IS NOT NULL
          AND textHash <> 0
        GROUP BY textHash
        ORDER BY textHash
        """,
        tuple(talk_ids),
    ).fetchall()
    payload = "|".join(f"{text_hash}:{count}" for text_hash, count in rows)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _upsert_quest_text_signature(cursor, quest_id, title_text_map_hash, dialogue_signature):
    cursor.execute(
        "INSERT INTO quest_text_signature(questId, titleTextMapHash, dialogue_signature) VALUES (?,?,?) "
        "ON CONFLICT(questId) DO UPDATE SET "
        "titleTextMapHash=excluded.titleTextMapHash, "
        "dialogue_signature=excluded.dialogue_signature "
        "WHERE "
        "NOT (quest_text_signature.dialogue_signature IS excluded.dialogue_signature)",
        (quest_id, title_text_map_hash, dialogue_signature),
    )


def importQuest(
    fileName: str,
    current_version: str | None = None,
    *,
    cursor=None,
    write_versions: bool = True,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)
    obj = _load_json_file(os.path.join(DATA_PATH, "BinOutput", "Quest", fileName))

    version_id: int | None = None
    if write_versions:
        version = current_version or get_current_version()
        version_id = get_or_create_version_id(version)
    sql1 = (
        "INSERT INTO quest(questId, titleTextMapHash, chapterId, created_version_id) "
        "VALUES (?,?,?,?) "
        "ON CONFLICT(questId) DO UPDATE SET "
        "titleTextMapHash=excluded.titleTextMapHash, "
        "chapterId=excluded.chapterId, "
        "created_version_id=CASE "
        "WHEN excluded.created_version_id IS NULL THEN quest.created_version_id "
        "WHEN quest.created_version_id IS NULL THEN excluded.created_version_id "
        "WHEN excluded.created_version_id < quest.created_version_id THEN excluded.created_version_id "
        "ELSE quest.created_version_id "
        "END "
        "WHERE "
        "NOT (quest.titleTextMapHash IS excluded.titleTextMapHash) "
        "OR NOT (quest.chapterId IS excluded.chapterId) "
        "OR quest.created_version_id IS NULL"
    )
    sql2 = "INSERT OR IGNORE INTO questTalk(questId, talkId) VALUES (?,?)"

    quest_row = extract_quest_row(obj)
    if quest_row is None:
        if skip_collector is not None:
            skip_collector.append(fileName)
        elif log_skip:
            print("Skipping " + fileName)
        if own_cursor:
            cursor.close()
        return None, False

    questId, titleTextMapHash, chapterId = quest_row
    if titleTextMapHash in (None, 0):
        titleTextMapHash = None
        if not _is_hidden_quest_obj(obj):
            if missing_title_collector is not None:
                missing_title_collector.append(f"{questId} ({fileName})")
            else:
                print("questId {} don't have TitleTextMapHash!".format(questId))
    if chapterId == 0:
        chapterId = None

    talk_ids = extract_quest_talk_ids(obj)
    if not talk_ids:
        if no_talk_collector is not None:
            no_talk_collector.append(f"{questId} ({fileName})")
        else:
            print("questId {} don't have talk!".format(questId))
    normalized_talk_ids = sorted(_normalize_talk_ids(talk_ids))

    new_signature = _build_quest_dialogue_signature(cursor, talk_ids)
    old_signature_row = cursor.execute(
        "SELECT dialogue_signature FROM quest_text_signature WHERE questId=?",
        (questId,),
    ).fetchone()

    # 检查dialogue_signature是否变更
    dialogue_changed = (
        old_signature_row is None
        or old_signature_row[0] != new_signature
    )

    # 检查titleTextMapHash对应内容是否变更
    title_changed = False
    if titleTextMapHash:
        # 获取当前titleTextMapHash对应的内容
        current_title_content = cursor.execute(
            "SELECT content FROM textMap WHERE hash=? LIMIT 1",
            (titleTextMapHash,)
        ).fetchone()

        # 获取旧的titleTextMapHash
        old_title_hash_row = cursor.execute(
            "SELECT titleTextMapHash FROM quest WHERE questId=?",
            (questId,)
        ).fetchone()

        if old_title_hash_row and old_title_hash_row[0]:
            # 获取旧的titleTextMapHash对应的内容
            old_title_content = cursor.execute(
                "SELECT content FROM textMap WHERE hash=? LIMIT 1",
                (old_title_hash_row[0],)
            ).fetchone()

            # 比较内容是否变更
            current_content = current_title_content[0] if current_title_content else None
            old_content = old_title_content[0] if old_title_content else None
            title_changed = current_content != old_content
        else:
            # 旧的titleTextMapHash不存在，视为变更
            title_changed = True

    # 文本内容变更包括dialogue变更或title变更
    text_changed = dialogue_changed or title_changed

    old_talk_rows = cursor.execute(
        "SELECT talkId FROM questTalk WHERE questId=? ORDER BY talkId",
        (questId,),
    ).fetchall()
    old_talk_ids = [row[0] for row in old_talk_rows]
    talk_links_changed = old_talk_ids != normalized_talk_ids

    old_version_row = cursor.execute(
        "SELECT created_version_id FROM quest WHERE questId=?",
        (questId,),
    ).fetchone()
    is_new_quest = old_version_row is None

    old_created_version = old_version_row[0] if old_version_row else None
    created_version = old_created_version

    # 版本预审查：只有当版本或内容发生变化时才执行SQL
    created_version_changed = should_update_version(old_created_version, created_version, is_created=True)

    if is_new_quest or text_changed or talk_links_changed or created_version_changed:
        cursor.execute(sql1, (questId, titleTextMapHash, chapterId, created_version))
    if talk_links_changed:
        cursor.execute("DELETE FROM questTalk WHERE questId=?", (questId,))
        cursor.executemany(sql2, ((questId, talkId) for talkId in normalized_talk_ids))

    _upsert_quest_text_signature(cursor, questId, titleTextMapHash, new_signature)



    if own_cursor:
        cursor.close()
    return questId, is_new_quest


def importAllQuests(
    current_version: str | None = None,
    sync_delete: bool = False,
    *,
    write_versions: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    version = None
    if write_versions:
        version = current_version or get_current_version()
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)

    quest_folder = os.path.join(DATA_PATH, "BinOutput", "Quest")
    files = os.listdir(quest_folder)
    imported_quest_ids = set()
    new_quest_ids = set()
    skipped_quest_files: list[str] = []
    missing_title_quests: list[str] = []
    no_talk_quests: list[str] = []

    try:
        with LightweightProgress(len(files), desc="Quest files", unit="files") as pbar:
            for i, fileName in enumerate(files):
                quest_id, is_new_quest = importQuest(
                    fileName,
                    current_version=version,
                    cursor=cursor,
                    write_versions=write_versions,
                    skip_collector=skipped_quest_files,
                    log_skip=False,
                    missing_title_collector=missing_title_quests,
                    no_talk_collector=no_talk_quests,
                )
                if quest_id is not None:
                    imported_quest_ids.add(quest_id)
                if is_new_quest and quest_id is not None:
                    new_quest_ids.add(quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                # 批量删除，减少数据库操作
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                params = tuple(imported_quest_ids)

                # 批量执行删除操作
                cursor.execute(f"DELETE FROM quest WHERE questId NOT IN ({placeholders})", params)
                cursor.execute(f"DELETE FROM questTalk WHERE questId NOT IN ({placeholders})", params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId NOT IN ({placeholders})", params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId NOT IN ({placeholders})", params)
            else:
                cursor.execute("DELETE FROM quest")
                cursor.execute("DELETE FROM questTalk")
                cursor.execute("DELETE FROM quest_text_signature")
                cursor.execute("DELETE FROM quest_hash_map")

        # 批量更新哈希映射
        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
        else:
            refreshed_hash_map_quests = 0

        # 批量回填版本信息
        if write_versions and imported_quest_ids:
            _backfill_quest_created_version_from_textmap(
                cursor,
                quest_ids=imported_quest_ids,
                overwrite_existing=False,
            )

        conn.commit()
    except Exception as e:
        print(f"Error in importAllQuests: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("quest", skipped_quest_files)
    _print_issue_summary("quest missing titleTextMapHash", missing_title_quests)
    _print_issue_summary("quest without talk ids", no_talk_quests)
    return {
        "files_total": len(files),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "skipped_file_count": len(skipped_quest_files),
        "skipped_file_samples": skipped_quest_files[:10],
        "missing_title_count": len(missing_title_quests),
        "no_talk_count": len(no_talk_quests),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def importTalk(
    fileName: str,
    *,
    cursor=None,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    refresh_hash_map: bool = True,
    touched_talk_collector: set[int] | None = None,
) -> int:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
    obj = _load_json_file(os.path.join(DATA_PATH, "BinOutput", "Talk", fileName))
    if _is_non_dialog_talk_obj(obj):
        if own_cursor:
            cursor.close()
        return 0

    if "talkId" in obj:
        talkIdKey = "talkId"
        dialogueListKey = "dialogList"
        dialogueIdKey = "id"
        talkRoleKey = "talkRole"
        talkRoleTypeKey = "type"
        talkRoleIdKey = "_id"
        talkContentTextMapHashKey = "talkContentTextMapHash"
    elif "ADHLLDAPKCM" in obj:
        talkIdKey = "ADHLLDAPKCM"
        dialogueListKey = "MOEOFGCKILF"
        dialogueIdKey = "ILHDNJDDEOP"
        talkRoleKey = "LCECPDILLEE"
        talkRoleTypeKey = "_type"
        talkRoleIdKey = "_id"
        talkContentTextMapHashKey = "GABLFFECBDO"
    elif "FEOACBMDCKJ" in obj and "AAOAAFLLOJI" in obj:
        talkIdKey = "FEOACBMDCKJ"
        dialogueListKey = "AAOAAFLLOJI"
        dialogueIdKey = "CCFPGAKINNB"
        talkRoleKey = "HJLEMJIGNFE"
        talkRoleTypeKey = "type"
        talkRoleIdKey = "id"
        talkContentTextMapHashKey = "BDOKCLNNDGN"
    elif "LBPGKDMGFBN" in obj and "LOJEOMAPIIM" in obj:
        talkIdKey = "LBPGKDMGFBN"
        dialogueListKey = "LOJEOMAPIIM"
        dialogueIdKey = "BLKKAMEMBBJ"
        talkRoleKey = "HJIPOJOECIF"
        talkRoleTypeKey = "_type"
        talkRoleIdKey = "_id"
        talkContentTextMapHashKey = "CMKPOJOEHHA"
    else:
        if skip_collector is not None:
            skip_collector.append(fileName)
        elif log_skip:
            print("Skipping " + fileName)
        if own_cursor:
            cursor.close()
        return 0

    talkId = obj[talkIdKey]
    if dialogueListKey not in obj or len(obj[dialogueListKey]) == 0:
        if own_cursor:
            cursor.close()
        return 0

    sql = (
        "INSERT INTO dialogue(dialogueId, talkerId, talkerType, talkId, textHash, coopQuestId) "
        "VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(dialogueId) DO UPDATE SET "
        "talkerId=excluded.talkerId, "
        "talkerType=excluded.talkerType, "
        "talkId=excluded.talkId, "
        "textHash=excluded.textHash, "
        "coopQuestId=excluded.coopQuestId "
        "WHERE "
        "NOT (dialogue.talkerId IS excluded.talkerId) "
        "OR NOT (dialogue.talkerType IS excluded.talkerType) "
        "OR NOT (dialogue.talkId IS excluded.talkId) "
        "OR NOT (dialogue.textHash IS excluded.textHash) "
        "OR NOT (dialogue.coopQuestId IS excluded.coopQuestId)"
    )

    coopMatch = re.match(r"^Coop[\\,/]([0-9]+)_[0-9]+.json$", fileName)
    if coopMatch:
        coopQuestId = coopMatch.group(1)
    else:
        coopQuestId = None

    rows = []
    for dialogue in obj[dialogueListKey]:
        dialogueId = dialogue.get(dialogueIdKey)
        if dialogueId is None:
            continue
        if talkRoleKey in dialogue and talkRoleIdKey in dialogue[talkRoleKey] and talkRoleTypeKey in dialogue[talkRoleKey]:
            talkRoleId = dialogue[talkRoleKey][talkRoleIdKey]
            talkRoleType = dialogue[talkRoleKey][talkRoleTypeKey]
        else:
            talkRoleId = -1
            talkRoleType = None

        if talkContentTextMapHashKey not in dialogue:
            continue
        textHash = dialogue[talkContentTextMapHashKey]
        rows.append((dialogueId, talkRoleId, talkRoleType, talkId, textHash, coopQuestId))

    if rows:
        executemany_batched(cursor, sql, rows, batch_size=batch_size)
    if touched_talk_collector is not None:
        try:
            touched_talk_collector.add(int(talkId))
        except Exception:
            pass
    if refresh_hash_map:
        _refresh_quest_hash_map_for_talk_ids(cursor, [talkId], batch_size=batch_size)

    if own_cursor:
        cursor.close()
        if commit:
            conn.commit()
    return len(rows)


def _is_non_dialog_talk_obj(obj: dict) -> bool:
    if not isinstance(obj, dict):
        return False
    keys = set(obj.keys())

    if keys == {"activityId", "talks"}:
        return True
    if "talks" in obj and isinstance(obj.get("talks"), list):
        return True

    if "DGJMIPFDEOF" in obj and isinstance(obj.get("DGJMIPFDEOF"), list):
        if (
            "CAKFHGJGEEK" in obj
            or "BLPHCANGKPL" in obj
            or "EOFLGOBJBCG" in obj
            or "configId" in obj
            or "groupId" in obj
            or "npcId" in obj
        ):
            return True

    if "DLPKMDPABFM" in obj and "LBPGKDMGFBN" in obj:
        if not isinstance(obj.get("LOJEOMAPIIM"), list):
            return True

    if "AFKIEPNELHE" in obj and "IKCBIFLCCOH" in obj and "PDFCHAAMEHA" in obj:
        return True
    if "AFNAKLCPGNF" in obj and "speed" in obj and "maxSpeed" in obj:
        return True
    if "FDAAMLIPKAK" in obj and "reApplyModifierOnStateChange" in obj:
        return True

    return False


def importAllTalkItems(
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    talk_root = os.path.join(DATA_PATH, "BinOutput", "Talk")
    if not os.path.isdir(talk_root):
        print("Talk folder not found, skipping.")
        return 0

    imported_rows = 0
    talk_files: list[str] = []
    skipped_files: list[str] = []
    touched_talk_ids: set[int] = set()

    # 预收集所有文件路径，减少I/O操作
    folders = sorted(os.listdir(talk_root))
    for folder in folders:
        folder_path = os.path.join(talk_root, folder)
        if not os.path.isdir(folder_path):
            continue
        for file_name in sorted(os.listdir(folder_path)):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                talk_files.append(folder + "\\" + file_name)

    print(f"importing talk files ({len(talk_files)})")
    cursor = conn.cursor()

    # 批量处理优化：减少事务提交次数
    try:
        with LightweightProgress(len(talk_files), desc="Talk files", unit="files") as pbar:
            for file_name in talk_files:
                imported_rows += importTalk(
                    file_name,
                    cursor=cursor,
                    commit=False,
                    batch_size=batch_size,
                    skip_collector=skipped_files,
                    log_skip=False,
                    refresh_hash_map=False,
                    touched_talk_collector=touched_talk_ids,
                )
                pbar.update()

        # 批量更新哈希映射，减少数据库操作
        if touched_talk_ids:
            _refresh_quest_hash_map_for_talk_ids(
                cursor,
                touched_talk_ids,
                batch_size=batch_size,
            )

        if commit:
            conn.commit()
    except Exception as e:
        print(f"Error in importAllTalkItems: {e}")
        if commit:
            conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("talk", skipped_files)
    return imported_rows


def importQuestBriefs(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    folder = os.path.join(DATA_PATH, "BinOutput", "QuestBrief")
    if not os.path.isdir(folder):
        print("QuestBrief folder not found, skipping.")
        cursor.close()
        return

    sql = "INSERT OR IGNORE INTO questTalk(questId, talkId) VALUES (?,?)"
    files = os.listdir(folder)
    touched_quest_ids: set[int] = set()

    def _iter_rows():
        with LightweightProgress(len(files), desc="QuestBrief files", unit="files") as pbar:
            for i, fileName in enumerate(files):
                if not fileName.endswith(".json"):
                    pbar.update()
                    continue
                try:
                    obj = _load_json_file(os.path.join(folder, fileName))
                except Exception:
                    pbar.update()
                    continue

                questId = extract_quest_id(obj)

                subquests = None
                if "subQuests" in obj:
                    subquests = obj["subQuests"]
                elif "GFLHMKOOHHA" in obj:
                    subquests = obj["GFLHMKOOHHA"]
                elif "NLCNGJKMAEN" in obj:
                    subquests = obj["NLCNGJKMAEN"]

                if not isinstance(subquests, list):
                    pbar.update()
                    continue

                for subquest in subquests:
                    mainQuestId = questId
                    if mainQuestId is None:
                        if "mainQuestId" in subquest:
                            mainQuestId = subquest["mainQuestId"]
                        elif "GNGFBMPFBOK" in subquest:
                            mainQuestId = subquest["GNGFBMPFBOK"]
                        elif "JKHGFFKOFFN" in subquest:
                            mainQuestId = subquest["JKHGFFKOFFN"]

                    contents = None
                    if "finishCond" in subquest:
                        contents = subquest["finishCond"]
                    elif "KBFJAAFDHKJ" in subquest:
                        contents = subquest["KBFJAAFDHKJ"]
                    elif "AACKELGGJGC" in subquest:
                        contents = subquest["AACKELGGJGC"]

                    if not isinstance(contents, list):
                        continue

                    for cond in contents:
                        cond_type = None
                        if "type" in cond:
                            cond_type = cond["type"]
                        elif "PAINLIBBLDK" in cond:
                            cond_type = cond["PAINLIBBLDK"]
                        elif "DLPKMDPABFM" in cond:
                            cond_type = cond["DLPKMDPABFM"]

                        if cond_type != "QUEST_CONTENT_COMPLETE_TALK":
                            continue

                        params = None
                        if "param" in cond:
                            params = cond["param"]
                        elif "paramList" in cond:
                            params = cond["paramList"]
                        elif "LNHLPKELCAL" in cond:
                            params = cond["LNHLPKELCAL"]
                        elif "IEKGEJMAOCN" in cond:
                            params = cond["IEKGEJMAOCN"]

                        if not isinstance(params, list) or len(params) == 0:
                            continue
                        talkId = params[0]
                        if isinstance(talkId, int) and talkId > 0 and mainQuestId:
                            try:
                                touched_quest_ids.add(int(mainQuestId))
                            except Exception:
                                pass
                            yield (mainQuestId, talkId)
                pbar.update()

    executemany_batched(cursor, sql, _iter_rows(), batch_size=batch_size)
    _refresh_quest_hash_map_for_quest_ids(
        cursor,
        touched_quest_ids,
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def refreshQuestHashMapByTalkIds(
    talk_ids,
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    refreshed = _refresh_quest_hash_map_for_talk_ids(
        cursor,
        talk_ids,
        batch_size=batch_size,
    )
    cursor.close()
    if commit:
        conn.commit()
    return int(refreshed or 0)


def refreshQuestHashMapByQuestIds(
    quest_ids,
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    refreshed = _refresh_quest_hash_map_for_quest_ids(
        cursor,
        quest_ids,
        batch_size=batch_size,
    )
    cursor.close()
    if commit:
        conn.commit()
    return int(refreshed or 0)


def runQuestOnly(
    *,
    current_version: str | None = None,
    write_versions: bool = False,
    prune_missing: bool = True,
    include_quests: bool = True,
    include_talks: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    talk_rows = 0
    if include_talks:
        talk_rows = importAllTalkItems(commit=True, batch_size=batch_size)

    if include_quests:
        quest_stats = importAllQuests(
            current_version=current_version,
            sync_delete=prune_missing,
            write_versions=write_versions,
        )
        importQuestBriefs(commit=True, batch_size=batch_size)
    else:
        quest_stats = {
            "files_total": 0,
            "imported_quest_count": 0,
            "new_quest_count": 0,
            "skipped_file_count": 0,
            "skipped_file_samples": [],
            "missing_title_count": 0,
            "no_talk_count": 0,
        }

    result = dict(quest_stats or {})
    result["talk_rows_imported"] = int(talk_rows or 0)
    result["quests_processed"] = bool(include_quests)
    result["talks_processed"] = bool(include_talks)
    return result
