from datetime import datetime, timedelta
from threading import RLock


class SearchCache:
    def __init__(self, max_size=1000, expiration_minutes=30):
        self.cache = {}
        self.max_size = max_size
        self.expiration = timedelta(minutes=expiration_minutes)
        self.version = 1  # 缓存版本号，用于在修复bug后自动刷新缓存
        self._lock = RLock()
    
    def get(self, key):
        with self._lock:
            versioned_key = f"v{self.version}:{key}"
            item = self.cache.get(versioned_key)
            if item is None:
                return None
            if datetime.now() > item['expires']:
                del self.cache[versioned_key]
                return None
            return item['value']
    
    def set(self, key, value):
        with self._lock:
            versioned_key = f"v{self.version}:{key}"
            if versioned_key not in self.cache and len(self.cache) >= self.max_size:
                oldest_key = min(self.cache, key=lambda k: self.cache[k]['created'])
                del self.cache[oldest_key]
            self.cache[versioned_key] = {
                'value': value,
                'created': datetime.now(),
                'expires': datetime.now() + self.expiration
            }
    
    def clear(self):
        with self._lock:
            self.cache.clear()
    
    def size(self):
        with self._lock:
            return len(self.cache)
    
    def increment_version(self):
        """递增缓存版本号，实现缓存的自动刷新"""
        with self._lock:
            self.version += 1
            self.clear()


# 创建全局搜索缓存实例
search_cache = SearchCache(max_size=1000, expiration_minutes=30)

# 当修复bug后，调用search_cache.increment_version()来自动刷新缓存
