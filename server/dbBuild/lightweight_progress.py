import time

class LightweightProgress:
    def __init__(self, total, desc="", unit=""):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        print(f"{desc}: 0/{total} {unit}")

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
            print(f"{self.desc}: {self.current}/{self.total} {self.unit} - 剩余时间: {remaining_str}")

    def set_postfix_str(self, s):
        pass
