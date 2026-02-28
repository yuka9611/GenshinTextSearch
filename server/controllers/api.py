from flask import Blueprint, request, jsonify
from databaseHelper import selectTextMapFromKeywordPaged, selectVoiceFromKeywordPaged, getVoicePath, getTextMapByHash, getVersionData
from utils.helpers import getLangFromRequest, normalizeSearchTerm, getLanguageName
from utils.cache import SearchCache

api_bp = Blueprint('api', __name__)

# 初始化搜索缓存
search_cache = SearchCache(max_size=1000)

@api_bp.route('/api/search', methods=['GET'])
def searchText():
    keyword = request.args.get('keyword', '', type=str)
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 10, type=int)
    lang = getLangFromRequest()
    
    # 生成缓存键
    cache_key = f"search:{keyword}:{page}:{size}:{lang}"
    
    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    # 标准化搜索词
    normalized_keyword = normalizeSearchTerm(keyword)
    
    # 执行搜索
    results = selectTextMapFromKeywordPaged(normalized_keyword, page, size, lang)
    
    # 构建响应
    response = {
        'keyword': keyword,
        'lang': lang,
        'page': page,
        'size': size,
        'total': len(results),
        'results': results
    }
    
    # 缓存结果
    search_cache.set(cache_key, response)
    
    return jsonify(response)

@api_bp.route('/api/voice', methods=['GET'])
def searchVoice():
    keyword = request.args.get('keyword', '', type=str)
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 10, type=int)
    lang = getLangFromRequest()
    
    # 生成缓存键
    cache_key = f"voice:{keyword}:{page}:{size}:{lang}"
    
    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    # 标准化搜索词
    normalized_keyword = normalizeSearchTerm(keyword)
    
    # 执行搜索
    results = selectVoiceFromKeywordPaged(normalized_keyword, page, size, lang)
    
    # 构建响应
    response = {
        'keyword': keyword,
        'lang': lang,
        'page': page,
        'size': size,
        'total': len(results),
        'results': results
    }
    
    # 缓存结果
    search_cache.set(cache_key, response)
    
    return jsonify(response)

@api_bp.route('/api/voice/path', methods=['GET'])
def getVoicePathApi():
    voice_hash = request.args.get('hash', '', type=str)
    lang = getLangFromRequest()
    
    # 生成缓存键
    cache_key = f"voice_path:{voice_hash}:{lang}"
    
    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    # 获取语音路径
    path = getVoicePath(voice_hash, lang)
    
    # 构建响应
    response = {
        'hash': voice_hash,
        'lang': lang,
        'path': path
    }
    
    # 缓存结果
    search_cache.set(cache_key, response)
    
    return jsonify(response)

@api_bp.route('/api/textmap', methods=['GET'])
def getTextMapApi():
    hash_val = request.args.get('hash', '', type=str)
    lang = getLangFromRequest()
    
    # 生成缓存键
    cache_key = f"textmap:{hash_val}:{lang}"
    
    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    # 获取文本映射
    text = getTextMapByHash(hash_val, lang)
    
    # 构建响应
    response = {
        'hash': hash_val,
        'lang': lang,
        'text': text
    }
    
    # 缓存结果
    search_cache.set(cache_key, response)
    
    return jsonify(response)

@api_bp.route('/api/version', methods=['GET'])
def getVersionApi():
    # 生成缓存键
    cache_key = "version_data"
    
    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    # 获取版本数据
    version_data = getVersionData()
    
    # 缓存结果
    search_cache.set(cache_key, version_data)
    
    return jsonify(version_data)

@api_bp.route('/api/languages', methods=['GET'])
def getLanguagesApi():
    # 生成缓存键
    cache_key = "languages"
    
    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)
    
    # 构建语言列表
    languages = [
        {"code": "zh-cn", "name": getLanguageName("zh-cn")},
        {"code": "en-us", "name": getLanguageName("en-us")},
        {"code": "ja-jp", "name": getLanguageName("ja-jp")},
        {"code": "ko-kr", "name": getLanguageName("ko-kr")}
    ]
    
    # 缓存结果
    search_cache.set(cache_key, languages)
    
    return jsonify(languages)
