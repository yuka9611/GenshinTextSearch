import { ref } from 'vue'

class RequestCache {
  constructor() {
    this.cache = new Map()
    this.maxSize = 100 // 最大缓存数量
  }

  /**
   * 生成缓存键
   * @param {string} url - 请求URL
   * @param {Object} params - 请求参数
   * @returns {string} 缓存键
   */
  generateKey(url, params) {
    const key = `${url}_${JSON.stringify(params || {})}`
    return key
  }

  /**
   * 获取缓存数据
   * @param {string} url - 请求URL
   * @param {Object} params - 请求参数
   * @returns {any|null} 缓存的数据，如果不存在返回null
   */
  get(url, params) {
    const key = this.generateKey(url, params)
    const cachedItem = this.cache.get(key)
    
    if (cachedItem) {
      // 检查缓存是否过期（5分钟过期）
      const now = Date.now()
      if (now - cachedItem.timestamp < 5 * 60 * 1000) {
        return cachedItem.data
      } else {
        // 缓存过期，删除
        this.cache.delete(key)
        return null
      }
    }
    return null
  }

  /**
   * 设置缓存数据
   * @param {string} url - 请求URL
   * @param {Object} params - 请求参数
   * @param {any} data - 要缓存的数据
   */
  set(url, params, data) {
    const key = this.generateKey(url, params)
    
    // 检查缓存大小，如果超过最大限制，删除最旧的缓存
    if (this.cache.size >= this.maxSize) {
      const oldestKey = this.getOldestKey()
      if (oldestKey) {
        this.cache.delete(oldestKey)
      }
    }
    
    this.cache.set(key, {
      data,
      timestamp: Date.now()
    })
  }

  /**
   * 获取最旧的缓存键
   * @returns {string|null} 最旧的缓存键
   */
  getOldestKey() {
    let oldestKey = null
    let oldestTimestamp = Infinity
    
    for (const [key, item] of this.cache.entries()) {
      if (item.timestamp < oldestTimestamp) {
        oldestTimestamp = item.timestamp
        oldestKey = key
      }
    }
    
    return oldestKey
  }

  /**
   * 清除缓存
   * @param {string} url - 请求URL，如果提供则只清除该URL的缓存，否则清除所有缓存
   */
  clear(url) {
    if (url) {
      // 清除指定URL的所有缓存
      for (const key of this.cache.keys()) {
        if (key.startsWith(url)) {
          this.cache.delete(key)
        }
      }
    } else {
      // 清除所有缓存
      this.cache.clear()
    }
  }

  /**
   * 获取缓存大小
   * @returns {number} 缓存大小
   */
  size() {
    return this.cache.size
  }
}

// 创建全局缓存实例
const requestCache = new RequestCache()

// 缓存装饰器，用于包装API请求函数
export const withCache = (fn, cacheKeyFn) => {
  return async (...args) => {
    // 生成缓存键
    const cacheKey = cacheKeyFn ? cacheKeyFn(...args) : fn.name + '_' + JSON.stringify(args)
    
    // 尝试从缓存获取
    const cachedData = requestCache.get(cacheKey, {})
    if (cachedData) {
      return cachedData
    }
    
    // 执行请求
    const data = await fn(...args)
    
    // 缓存结果
    requestCache.set(cacheKey, {}, data)
    
    return data
  }
}

export default requestCache
