import os
import re
from tqdm import tqdm
import json
from DBConfig import conn, LANG_PATH


def importTextMap(baseMapName: str, fileList: list):
    """
    :param baseMapName: 数据库 langCode 表中对应的 codeName (例如 TextMapRU.json)
    :param fileList: 实际要读取的文件列表 (例如 ['TextMapRU_0.json', 'TextMapRU_1.json'])
    """
    cursor = conn.cursor()

    # 1. 检查 baseMapName 是否在 langCode 表中
    sql2 = "select id,imported from langCode where codeName = ?"
    cursor.execute(sql2, (baseMapName,))
    ans2 = cursor.fetchall()
    if len(ans2) == 0:
        print("{} (Base for {}) 不是预定义的语言文件，已跳过".format(baseMapName, fileList))
        return

    langId = ans2[0][0]
    imported = ans2[0][1]

    # 2. 检查导入状态
    if imported == 1:
        ans = input("{} 似乎已经导入到数据库了，要重新导入吗？输入y清空该语言并重新导入，输入n取消该语言的导入: ".format(baseMapName))
        if ans != 'y':
            return

    # 3. 清空旧数据 (只执行一次)
    print(f"正在清空 {baseMapName} (ID: {langId}) 的旧数据...")
    sql1 = 'delete from textMap where lang=?'
    cursor.execute(sql1, (langId,))

    # 4. 循环读取所有分卷文件并插入数据
    sql3 = "insert or ignore into textMap(hash, content, lang) values (?,?,?)"
    
    for fileName in fileList:
        filePath = os.path.join(LANG_PATH, fileName)
        if not os.path.exists(filePath):
            print(f"文件不存在: {filePath}")
            continue

        print(f"正在读取文件: {fileName} ...")
        try:
            textMap = json.load(open(filePath, encoding='utf-8'))
            print(f"正在导入 {fileName} 的数据...")
            
            # 使用 tqdm 显示进度
            for hashVal, content in tqdm(textMap.items(), total=len(textMap), desc=fileName):
                cursor.execute(sql3, (hashVal, content, langId))
                
        except Exception as e:
            print(f"读取或导入 {fileName} 时发生错误: {e}")

    # 5. 设置为已导入状态
    sql4 = 'update langCode set imported=1 where id=?'
    cursor.execute(sql4, (langId,))

    cursor.close()
    conn.commit()
    print(f"完成 {baseMapName} 的导入。")


def importAllTextMap():
    if not os.path.exists(LANG_PATH):
        print(f"语言文件夹 {LANG_PATH} 不存在")
        return

    files = os.listdir(LANG_PATH)
    
    # 分组字典: { 'TextMapRU.json': ['TextMapRU_0.json', 'TextMapRU_1.json'], 'TextMapCHS.json': ['TextMapCHS.json'] }
    file_groups = {}

    for fileName in files:
        if not fileName.endswith(".json"):
            continue

        # 使用正则提取基础名称
        # 匹配: TextMapXX_0.json 或 TextMapXX.json -> 提取 TextMapXX
        match = re.match(r"^(TextMap[a-zA-Z]+)(?:_\d+)?\.json$", fileName)
        
        if match:
            # 构造基础名称，例如 TextMapRU -> TextMapRU.json
            base_name = match.group(1) + ".json"
            
            if base_name not in file_groups:
                file_groups[base_name] = []
            file_groups[base_name].append(fileName)
        else:
            print(f"跳过不符合命名规则的文件: {fileName}")

    # 遍历每个组进行导入
    for baseMapName, fileList in file_groups.items():
        # 排序文件，确保按 0, 1, 2 顺序读取 (虽然对 Hash Map 影响不大，但更稳妥)
        fileList.sort()
        importTextMap(baseMapName, fileList)


if __name__ == "__main__":
    importAllTextMap()