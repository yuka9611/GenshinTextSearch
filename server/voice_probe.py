import argparse
import os

from AudioReader.FilePackager import Package, fnv_hash_32, fnv_hash_64

import config
import languagePackReader


ROOT_MODES = (0, 1, 2)
SAMPLE_PATHS = [
    r"VO_friendship\VO_ayaka\vo_ayaka_spice_pref_01.wem",
    r"VO_LQ\VO_kachina\vo_MWKLQ002_10_kachina_01.wem",
    r"VO_EQ\VO_paimon\vo_EQFH004_6_paimon_01.wem",
    r"VO_WQ\VO_paimon\vo_RCJWQ002_17_paimon_02.wem",
    r"VO_tips\vo_tips_mug\vo_tips_MUG003_6_itto_08.wem",
    r"VO_gadget\VO_conch\vo_event_conch_record1_albedo_01.wem",
    r"VO_freetalk\VO_chongyun\vo_dialog_EQZYJ001_chongyun_01.wem",
    r"VO_AQ\GNR01_REMIXED_M.wem",
]


def _candidate_keys(lang_name: str, voice_path: str) -> list[tuple[str, str]]:
    slash_path = voice_path.replace("\\", "/")
    return [
        ("lang+path", f"{lang_name}\\{voice_path}"),
        ("path", voice_path),
        ("path-lower", voice_path.lower()),
        ("slash-path", slash_path),
        ("lang/slash-path", f"{lang_name}/{slash_path}"),
        ("basename", os.path.basename(voice_path)),
    ]


def _load_package_from_dir(path_dir: str) -> tuple[Package | None, int]:
    if not os.path.isdir(path_dir):
        return None, 0

    files = [
        os.path.join(path_dir, name)
        for name in sorted(os.listdir(path_dir))
        if name.lower().endswith(".pck")
    ]
    if not files:
        return None, 0

    package = Package()
    loaded = 0
    for file_path in files:
        try:
            fobj = open(file_path, "rb")
        except OSError:
            continue
        try:
            package.addfile(fobj)
        except Exception:
            fobj.close()
            continue
        loaded += 1

    if loaded == 0:
        return None, 0
    return package, loaded


def _probe_language_loader(voice_path: str) -> dict[int, bool]:
    return {
        lang_code: languagePackReader.checkAudioBin(voice_path, lang_code)
        for lang_code in sorted(languagePackReader.langPackages.keys())
    }


def _probe_loose_files(asset_dir: str, voice_path: str) -> dict[str, bool]:
    hits: dict[str, bool] = {}
    for base_dir in (
        os.path.join(asset_dir, "Persistent", "AudioAssets"),
        os.path.join(asset_dir, "StreamingAssets", "AudioAssets"),
    ):
        for lang_code, lang_name in languagePackReader.langCodes.items():
            full_path = os.path.join(base_dir, lang_name, voice_path)
            hits[f"{lang_code}:{full_path}"] = os.path.isfile(full_path)
    return hits


def _probe_root_packages(asset_dir: str, voice_path: str) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {}
    for base_dir in (
        os.path.join(asset_dir, "Persistent", "AudioAssets"),
        os.path.join(asset_dir, "StreamingAssets", "AudioAssets"),
    ):
        package, loaded = _load_package_from_dir(base_dir)
        if package is None:
            results[base_dir] = ["no-packages"]
            continue

        hits = [f"loaded={loaded}"]
        for key_label, key_value in _candidate_keys("Chinese", voice_path):
            hash_variants = (
                ("fnv64", fnv_hash_64(key_value)),
                ("fnv32", fnv_hash_32(key_value)),
            )
            for hash_name, hash_value in hash_variants:
                for mode in ROOT_MODES:
                    matched_langs = [
                        str(lang_id)
                        for lang_id, hash_map in package.map[mode].items()
                        if hash_value in hash_map
                    ]
                    if matched_langs:
                        hits.append(
                            f"{key_label}:{hash_name}:mode{mode}:langs={','.join(matched_langs)}"
                        )
        results[base_dir] = hits
    return results


def probe_voice_path(asset_dir: str, voice_path: str) -> dict[str, object]:
    return {
        "voicePath": voice_path,
        "languageLoader": _probe_language_loader(voice_path),
        "looseFiles": _probe_loose_files(asset_dir, voice_path),
        "rootPackages": _probe_root_packages(asset_dir, voice_path),
        "attemptedKeys": {
            lang_name: [value for _label, value in _candidate_keys(lang_name, voice_path)]
            for lang_name in sorted(set(languagePackReader.langCodes.values()))
        },
    }


def _format_bool(value: bool) -> str:
    return "hit" if value else "miss"


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe local audio asset coverage for one or more voicePath values.")
    parser.add_argument("voice_paths", nargs="*", help="voicePath values to probe")
    args = parser.parse_args()

    asset_dir = config.getAssetDir()
    if not config.isAssetDirValid():
        print("assetDir is not valid in server/config.json")
        return 1

    voice_paths = args.voice_paths or SAMPLE_PATHS
    print(f"assetDir: {asset_dir}")
    print(f"loadedLangs: {sorted(languagePackReader.langPackages.keys())}")

    for voice_path in voice_paths:
        result = probe_voice_path(asset_dir, voice_path)
        print("")
        print(f"[voice] {result['voicePath']}")
        print("  languageLoader:")
        language_loader = result["languageLoader"]
        if isinstance(language_loader, dict):
            for lang_code, matched in language_loader.items():
                print(f"    {lang_code}: {_format_bool(matched)}")
        else:
            print(f"    {language_loader}")
        print("  looseFiles:")
        loose_files = result["looseFiles"]
        if isinstance(loose_files, dict):
            for path_key, exists in loose_files.items():
                print(f"    {path_key}: {_format_bool(exists)}")
        else:
            print(f"    {loose_files}")
        print("  rootPackages:")
        root_packages = result["rootPackages"]
        if isinstance(root_packages, dict):
            for base_dir, hits in root_packages.items():
                print(f"    {base_dir}")
                for hit in hits:
                    print(f"      {hit}")
        else:
            print(f"    {root_packages}")
        print("  attemptedKeys:")
        attempted_keys = result["attemptedKeys"]
        if isinstance(attempted_keys, dict):
            for lang_name, keys in attempted_keys.items():
                print(f"    {lang_name}")
                for key in keys:
                    print(f"      {key}")
        else:
            print(f"    {attempted_keys}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
