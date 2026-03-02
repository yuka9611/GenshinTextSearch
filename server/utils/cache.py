from datetime import datetime, timedelta


class SearchCache:
    def __init__(self, max_size=1000, expiration_minutes=30):
        self.cache = {}
        self.max_size = max_size
        self.expiration = timedelta(minutes=expiration_minutes)
        self.version = 1  # 缓存版本号，用于在修复bug后自动刷新缓存
    
    def get(self, key):
        # 在缓存键中包含版本号
        versioned_key = f"v{self.version}:{key}"
        if versioned_key not in self.cache:
            return None
        item = self.cache[versioned_key]
        if datetime.now() > item['expires']:
            del self.cache[versioned_key]
            return None
        return item['value']
    
    def set(self, key, value):
        # 在缓存键中包含版本号
        versioned_key = f"v{self.version}:{key}"
        if len(self.cache) >= self.max_size:
            # 简单的LRU策略：删除最旧的项
            oldest_key = min(self.cache, key=lambda k: self.cache[k]['created'])
            del self.cache[oldest_key]
        self.cache[versioned_key] = {
            'value': value,
            'created': datetime.now(),
            'expires': datetime.now() + self.expiration
        }
    
    def clear(self):
        self.cache.clear()
    
    def size(self):
        return len(self.cache)
    
    def increment_version(self):
        """递增缓存版本号，实现缓存的自动刷新"""
        self.version += 1
        # 清空当前缓存，确保使用新的版本号
        self.clear()


# 创建全局搜索缓存实例
search_cache = SearchCache(max_size=1000, expiration_minutes=30)

# 当修复bug后，调用search_cache.increment_version()来自动刷新缓存