import time

class LightweightProgress:
    def __init__(self, total, desc="", unit=""):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        # 初始打印也使用\r格式, 确保与后续更新保持一致
        print(f"\r{desc}: 0/{total} {unit}", end='', flush=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _format_time(self, seconds):
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds // 60
            seconds = seconds % 60
            return f"{minutes}m {seconds:.0f}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"

    def update(self, n=1):
        self.current += n
        if self.current % 5 == 0 or self.current >= self.total:
            elapsed = time.time() - self.start_time
            if self.current > 0:
                progress_ratio = self.current / self.total
                total_time = elapsed / progress_ratio
                remaining = total_time - elapsed
                remaining_str = self._format_time(remaining)
            else:
                remaining_str = "N/A"
            # 使用\r实现同一位置刷新，end=''避免自动换行
            print(f"\r{self.desc}: {self.current}/{self.total} {self.unit} - 剩余时间: {remaining_str}", end='', flush=True)
            # 完成时添加换行
            if self.current >= self.total:
                print()

    def set_postfix_str(self, s):
        elapsed = time.time() - self.start_time
        if self.current > 0:
            progress_ratio = self.current / self.total
            total_time = elapsed / progress_ratio
            remaining = total_time - elapsed
            remaining_str = self._format_time(remaining)
        else:
            remaining_str = "N/A"
        # 显示带后缀的进度条
        print(f"\r{self.desc}: {self.current}/{self.total} {self.unit} - 剩余时间: {remaining_str} - {s}", end='', flush=True)
