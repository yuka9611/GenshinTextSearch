import os
import pkgutil
import sys
import time
import threading
import webbrowser
from importlib.util import find_spec

# Python 3.14 移除了 pkgutil.get_loader，旧版 Flask 仍会调用它。
if not hasattr(pkgutil, "get_loader"):
    def _compat_get_loader(name: str):
        module = sys.modules.get(name)
        if module is not None:
            loader = getattr(module, "__loader__", None)
            if loader is not None:
                return loader

            spec = getattr(module, "__spec__", None)
            if spec is not None:
                return spec.loader

            return None

        try:
            spec = find_spec(name)
        except (ImportError, AttributeError, ValueError):
            return None
        return None if spec is None else spec.loader

    pkgutil.get_loader = _compat_get_loader  # type: ignore[attr-defined]

from flask import Flask, send_from_directory

from cloud_runtime import (
    cors_origins,
    enforce_rate_limit,
    is_cloud_mode,
    trusted_proxy_enabled,
)
from utils.helpers import resource_path
from utils.browser_session import start_browser_session_watchdog


def _startup_profile_enabled() -> bool:
    return os.environ.get("GTS_STARTUP_PROFILE", "").strip() == "1"


def _log_startup_profile(label: str, elapsed_seconds: float) -> None:
    if _startup_profile_enabled():
        print(f"[startup-profile] {label}: {elapsed_seconds * 1000:.1f} ms", flush=True)


def create_app() -> Flask:
    """
    工厂模式：把重 import/初始化从模块顶层挪走，提升 PyInstaller 启动速度
    """
    started = time.perf_counter()
    app = Flask(__name__)

    # 延迟导入 CORS（减少顶层 import）
    cors_started = time.perf_counter()
    from flask_cors import CORS
    if is_cloud_mode():
        allowed_origins = cors_origins()
        if allowed_origins:
            CORS(
                app,
                resources={r"/api/*": {"origins": allowed_origins}},
                allow_headers=["Content-Type"],
                methods=["GET", "POST", "OPTIONS"],
            )
    else:
        CORS(app)

    if trusted_proxy_enabled():
        from werkzeug.middleware.proxy_fix import ProxyFix
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

    app.before_request(enforce_rate_limit)
    _log_startup_profile("configure CORS", time.perf_counter() - cors_started)

    # 导入并注册API蓝图
    api_started = time.perf_counter()
    from controllers.api import api_bp
    _log_startup_profile("import API blueprint", time.perf_counter() - api_started)
    register_started = time.perf_counter()
    app.register_blueprint(api_bp)
    _log_startup_profile("register API blueprint", time.perf_counter() - register_started)

    @app.route("/healthz")
    def healthcheck():
        return {"status": "ok", "cloudMode": is_cloud_mode()}

    if is_cloud_mode():
        @app.route("/")
        def serveCloudRoot():
            return {"service": "genshin-text-search-api", "status": "ok"}
    else:
        # ----------------------------
        # Static frontend (webui/dist)
        # ----------------------------
        staticDir = resource_path("webui/dist")

        @app.route("/")
        def serveRoot():
            return send_from_directory(staticDir, "index.html")

        @app.route("/<path:path>")
        def serveStatic(path):
            filePath = os.path.join(staticDir, path)
            if os.path.exists(filePath):
                return send_from_directory(staticDir, path)
            return send_from_directory(staticDir, "index.html")

    _log_startup_profile("create_app", time.perf_counter() - started)
    return app


def maybe_open_browser(url: str):
    """
    可控地打开浏览器：
    - 默认打开（方便发行版）
    - 若设置环境变量 GTS_NO_BROWSER=1 则不打开
    """
    if is_cloud_mode() or os.environ.get("GTS_NO_BROWSER", "").strip() == "1":
        return
    try:
        open_delay = max(0.0, float(os.environ.get("GTS_BROWSER_OPEN_DELAY", "0.3")))
    except ValueError:
        open_delay = 0.3

    def _open():
        # Give serve_forever a short moment to enter its accept loop.
        time.sleep(open_delay)
        try:
            webbrowser.open(url, new=1)
        except Exception:
            pass

    threading.Thread(target=_open, daemon=True).start()


def run_local_server(app: Flask, host: str, port: int) -> None:
    from werkzeug.serving import make_server

    server = make_server(host, port, app, threaded=True)
    start_browser_session_watchdog(app.logger, shutdown_callback=server.shutdown)
    maybe_open_browser(f"http://{host}:{port}/")

    try:
        server.serve_forever()
    finally:
        server.server_close()


class AssetDirPromptUnavailableError(RuntimeError):
    """Raised when the startup asset-dir prompt cannot be shown."""


def _pick_asset_dir_via_tkinter() -> str | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except ModuleNotFoundError as exc:
        if exc.name == "_tkinter":
            raise AssetDirPromptUnavailableError("_tkinter module is unavailable") from exc
        raise AssetDirPromptUnavailableError("tkinter is unavailable") from exc
    except Exception as exc:
        raise AssetDirPromptUnavailableError("tkinter is unavailable") from exc

    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        try:
            root.attributes("-topmost", True)
        except Exception:
            pass
        return filedialog.askdirectory(title="请选择原神资源目录（包含 StreamingAssets 或 Persistent）") or None
    except Exception as exc:
        raise AssetDirPromptUnavailableError("failed to open directory picker") from exc
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass


def _prompt_for_asset_dir_if_needed(config_module) -> None:
    if config_module.getAssetDir() and config_module.isAssetDirValid():
        return

    try:
        import tkinter as tk
        from tkinter import messagebox
    except ModuleNotFoundError as exc:
        if exc.name == "_tkinter":
            return
        return
    except Exception:
        return

    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        result = messagebox.askyesno("提示", "未找到有效的原神资源目录，是否现在选择？")
    except Exception:
        return
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass

    if not result:
        return

    try:
        picked = _pick_asset_dir_via_tkinter()
        if not picked:
            return
        config_module.setAssetDir(picked)
        config_module.saveConfig()
        if config_module.isAssetDirValid():
            return

        root = tk.Tk()
        root.withdraw()
        try:
            messagebox.showerror("错误", "所选路径不是有效的原神资源目录！\n请在设置页面重新选择。")
        finally:
            root.destroy()
    except Exception:
        try:
            root = tk.Tk()
            root.withdraw()
            try:
                messagebox.showerror("错误", "无法打开目录选择对话框！\n请在设置页面手动选择资源目录。")
            finally:
                root.destroy()
        except Exception:
            pass


if __name__ == "__main__":
    # 检查游戏目录是否存在且有效
    import config
    # 导入缓存模块，在启动时刷新缓存
    from utils.cache import search_cache
    # 递增缓存版本号，确保修复bug后缓存自动刷新
    search_cache.increment_version()

    # 如果没有游戏目录或目录无效，弹出提示框让用户选择
    if not is_cloud_mode():
        _prompt_for_asset_dir_if_needed(config)

    app = create_app()
    # 桌面发行版建议只监听本机
    run_local_server(app, host="127.0.0.1", port=5000)
