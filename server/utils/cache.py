from datetime import datetime, timedelta


class SearchCache:
    def __init__(self, max_size=1000, expiration_minutes=30):
        self.cache = {}
        self.max_size = max_size
        self.expiration = timedelta(minutes=expiration_minutes)
    
    def get(self, key):
        if key not in self.cache:
            return None
        item = self.cache[key]
        if datetime.now() > item['expires']:
            del self.cache[key]
            return None
        return item['value']
    
    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            # 简单的LRU策略：删除最旧的项
            oldest_key = min(self.cache, key=lambda k: self.cache[k]['created'])
            del self.cache[oldest_key]
        self.cache[key] = {
            'value': value,
            'created': datetime.now(),
            'expires': datetime.now() + self.expiration
        }
    
    def clear(self):
        self.cache.clear()
    
    def size(self):
        return len(self.cache)


# 创建全局搜索缓存实例
search_cache = SearchCache(max_size=1000, expiration_minutes=30)