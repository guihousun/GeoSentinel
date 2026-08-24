import uuid
import os

SESSION_DIR = "utils/sessions"  # 会话记录文件存储目录
os.makedirs(SESSION_DIR, exist_ok=True)

def init_session_state(session_state):
    """初始化会话状态，包括 thread_id 和 chat_history。"""
    if "thread_id" not in session_state:
        session_state.thread_id = str(uuid.uuid4())
    if "chat_history" not in session_state:
        session_state.chat_history = []

def reset_session(session_state):
    """重置当前会话，生成新的 thread_id 并清空聊天记录。"""
    session_state.thread_id = str(uuid.uuid4())
    session_state.chat_history = []

def save_history(session_state):
    """将当前会话历史保存到本地文件。"""
    file_path = os.path.join(SESSION_DIR, f"{session_state.thread_id}.txt")
    with open(file_path, "w", encoding="utf-8") as f:
        for role, content in session_state.chat_history:
            f.write(f"{role}: {content}\n")

def load_history(session_state, thread_id):
    """加载指定 thread_id 的历史记录，如果存在则恢复。"""
    file_path = os.path.join(SESSION_DIR, f"{thread_id}.txt")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
            history = [tuple(line.strip().split(": ", 1)) for line in lines if ": " in line]
        session_state.thread_id = thread_id
        session_state.chat_history = history
        return True
    return False

def export_history(session_state):
    """导出当前会话历史为字符串（可供下载）。"""
    return "\n".join([f"{role}: {content}" for role, content in session_state.chat_history])
