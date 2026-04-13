import time


class LightweightProgress:
    def __init__(
        self,
        total,
        desc="",
        unit="",
        initial_print=True,
        render_every=5,
        min_interval=0.5,
    ):
        self.total = total
        self.desc = desc
        self.unit = unit
        self.current = 0
        self.start_time = time.time()
        self.render_every = max(1, int(render_every))
        self.min_interval = max(0.0, float(min_interval))
        self._clear_line = "\x1b[2K\x1b[0G"
        self._last_render_state = None
        self._last_render_at = 0.0
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

    def _elapsed_time_str(self):
        return self._format_time(max(0.0, time.time() - self.start_time))

    def _rate_str(self):
        elapsed = max(0.0, time.time() - self.start_time)
        if elapsed <= 0.0 or self.current <= 0:
            return None
        rate = self.current / elapsed
        if rate >= 10:
            formatted = f"{rate:.0f}"
        elif rate >= 1:
            formatted = f"{rate:.1f}"
        else:
            formatted = f"{rate:.2f}"
        rate_unit = self.unit or "step"
        return f"{formatted} {rate_unit}/s"

    def _build_progress_str(self, postfix=None):
        progress_core = f"{self.current}/{self.total}"
        if self.unit:
            progress_core += f" {self.unit}"
        if self.total > 0:
            progress_core += f" ({self.current / self.total * 100:.1f}%)"

        progress_str = f"{self.desc}: {progress_core}"
        progress_str += f" - 已用: {self._elapsed_time_str()}"
        progress_str += f" - 剩余: {self._remaining_time_str()}"
        rate_str = self._rate_str()
        if rate_str:
            progress_str += f" - 速率: {rate_str}"
        if postfix:
            progress_str += f" - {postfix}"
        return progress_str

    def _render(self, postfix=None, *, force=False):
        now = time.time()
        normalized_postfix = postfix or None
        render_state = (
            self.current,
            self.total,
            self.desc,
            self.unit,
            normalized_postfix,
        )
        if (
            not force
            and render_state == self._last_render_state
            and now - self._last_render_at < self.min_interval
        ):
            return
        self._last_render_state = render_state
        self._last_render_at = now
        progress_str = self._build_progress_str(postfix=normalized_postfix)
        print(self._clear_line + progress_str, end="", flush=True)
        if self.current >= self.total:
            print()

    def update(self, n=1, postfix=None):
        self.current += n
        postfix_changed = (
            self._last_render_state is None
            or (postfix or None) != self._last_render_state[4]
        )
        should_render = (
            self.current >= self.total
            or self.current % self.render_every == 0
            or n == 0
            or postfix_changed
            or (time.time() - self._last_render_at) >= self.min_interval
        )
        if should_render:
            self._render(postfix=postfix, force=(n == 0 and postfix_changed))

    def set_postfix_str(self, s):
        self._render(postfix=s, force=True)
