import hashlib

def compare_files(file1_path, file2_path):
    """
    比较两个文件内容是否完全一致
    
    参数:
        file1_path (str): 第一个文件路径
        file2_path (str): 第二个文件路径
    
    返回:
        bool: 如果文件内容完全一致返回True，否则返回False
    """
    try:
        # 方法1：直接逐行比较（显示差异位置）
        with open(file1_path, 'r', encoding='utf-8') as f1, \
             open(file2_path, 'r', encoding='utf-8') as f2:
            
            line_num = 0
            for line1, line2 in zip(f1, f2):
                line_num += 1
                if line1 != line2:
                    print(f"差异在第 {line_num} 行:")
                    print(f"{file1_path}: {line1.strip()}")
                    print(f"{file2_path}: {line2.strip()}")
                    return False
            
            # 检查文件行数是否相同
            if f1.readline() or f2.readline():
                print("文件行数不同")
                return False
            
            return True

        # 方法2：使用MD5哈希比较（更快但不显示差异位置）
        # with open(file1_path, 'rb') as f1, open(file2_path, 'rb') as f2:
        #     return hashlib.md5(f1.read()).hexdigest() == hashlib.md5(f2.read()).hexdigest()

    except FileNotFoundError as e:
        print(f"错误: 文件未找到 - {e}")
        return False
    except Exception as e:
        print(f"错误: {e}")
        return False

# ================== 硬编码文件路径 ==================
FILE_1 = "output/result16-10M.txt"  # 替换为你的第一个文件路径
FILE_2 = "output/result16_10M.txt"  # 替换为你的第二个文件路径

if __name__ == "__main__":
    print(f"正在比较文件:\n1: {FILE_1}\n2: {FILE_2}")
    
    # 计算文件哈希值（快速校验）
    hash1 = hashlib.md5(open(FILE_1, 'rb').read()).hexdigest()
    hash2 = hashlib.md5(open(FILE_2, 'rb').read()).hexdigest()
    print(f"文件1 MD5: {hash1}")
    print(f"文件2 MD5: {hash2}")
    
    # 详细比较
    if compare_files(FILE_1, FILE_2):
        print("✅ 文件内容完全一致")
    else:
        print("❌ 文件内容不一致")