import time


class LightweightProgress:
    def __init__(self, total, desc="", unit="", initial_print=True):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        self._clear_line = "\x1b[2K\x1b[0G"
        self._last_render_state = None
        if initial_print:
            self._render()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _format_time(self, seconds):
        if seconds < 60:
            return f"{seconds:.1f}s"
        if seconds < 3600:
            minutes = seconds // 60
            seconds = seconds % 60
            return f"{minutes}m {seconds:.0f}s"
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}h {minutes}m"

    def _remaining_time_str(self):
        if self.current <= 0 or self.total <= 0:
            return "N/A"
        elapsed = time.time() - self.start_time
        progress_ratio = self.current / self.total
        total_time = elapsed / progress_ratio
        remaining = max(0.0, total_time - elapsed)
        return self._format_time(remaining)

    def _build_progress_str(self, postfix=None):
        progress_str = (
            f"{self.desc}: {self.current}/{self.total} {self.unit}"
            f" - 剩余时间: {self._remaining_time_str()}"
        )
        if postfix:
            progress_str += f" - {postfix}"
        return progress_str

    def _render(self, postfix=None):
        normalized_postfix = postfix or None
        render_state = (
            self.current,
            self.total,
            self.desc,
            self.unit,
            normalized_postfix,
        )
        if render_state == self._last_render_state:
            return
        self._last_render_state = render_state
        progress_str = self._build_progress_str(postfix=normalized_postfix)
        print(self._clear_line + progress_str, end="", flush=True)
        if self.current >= self.total:
            print()

    def update(self, n=1, postfix=None):
        self.current += n
        if self.current % 5 == 0 or self.current >= self.total or n == 0:
            self._render(postfix=postfix)

    def set_postfix_str(self, s):
        self._render(postfix=s)
