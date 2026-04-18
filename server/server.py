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

from utils.helpers import resource_path
from utils.browser_session import start_browser_session_watchdog


def create_app() -> Flask:
    """
    工厂模式：把重 import/初始化从模块顶层挪走，提升 PyInstaller 启动速度
    """
    app = Flask(__name__)

    # 延迟导入 CORS（减少顶层 import）
    from flask_cors import CORS
    CORS(app)

    # 导入并注册API蓝图
    from controllers.api import api_bp
    app.register_blueprint(api_bp)

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

    return app


def maybe_open_browser(url: str):
    """
    可控地打开浏览器：
    - 默认打开（方便发行版）
    - 若设置环境变量 GTS_NO_BROWSER=1 则不打开
    """
    if os.environ.get("GTS_NO_BROWSER", "").strip() == "1":
        return

    def _open():
        # 给 Flask 多一点时间起来（onefile 解压 + 初始化）
        time.sleep(1.5)
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


if __name__ == "__main__":
    # 检查游戏目录是否存在且有效
    import config
    # 导入缓存模块，在启动时刷新缓存
    from utils.cache import search_cache
    # 递增缓存版本号，确保修复bug后缓存自动刷新
    search_cache.increment_version()

    # 如果没有游戏目录或目录无效，弹出提示框让用户选择
    if not config.getAssetDir() or not config.isAssetDirValid():
        # 尝试导入tkinter和controllers模块
        try:
            import tkinter as tk
            from tkinter import messagebox
            import controllers as controllers_module

            # 询问用户是否选择游戏目录
            root = tk.Tk()
            root.withdraw()
            result = messagebox.askyesno("提示", "未找到有效的原神资源目录，是否现在选择？")
            root.destroy()

            if result:
                # 弹出目录选择对话框
                try:
                    picked = controllers_module.pickAssetDirViaDialog() # type: ignore
                    if picked:
                        config.setAssetDir(picked)
                        config.saveConfig()
                        # 再次检查目录是否有效
                        if not config.isAssetDirValid():
                            root = tk.Tk()
                            root.withdraw()
                            messagebox.showerror("错误", "所选路径不是有效的原神资源目录！\n请在设置页面重新选择。")
                            root.destroy()
                except Exception:
                    # 如果pickAssetDirViaDialog调用失败，显示错误信息
                    root = tk.Tk()
                    root.withdraw()
                    messagebox.showerror("错误", "无法打开目录选择对话框！\n请在设置页面手动选择游戏目录。")
                    root.destroy()
        except Exception:
            # 如果导入或操作失败，忽略错误
            pass

    app = create_app()
    # 桌面发行版建议只监听本机
    run_local_server(app, host="127.0.0.1", port=5000)
