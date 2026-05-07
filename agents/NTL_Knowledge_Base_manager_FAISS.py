import os
import json
from langchain_community.document_loaders import PyPDFLoader, WebBaseLoader, TextLoader, PythonLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document

if not os.getenv("OPENAI_API_KEY"):
    raise RuntimeError("OPENAI_API_KEY is required to build FAISS knowledge embeddings.")

class RAGDatabase:
    def __init__(self, persistent_directory, collection_name="knowledge-faiss"):
        """
        使用 FAISS 初始化 RAG 数据库
        :param persistent_directory: 持久化存储目录（FAISS 保存为本地文件）
        :param collection_name: 仅用于区分用途的命名，不影响 FAISS
        """
        self.persistent_directory = persistent_directory
        self.collection_name = collection_name
        os.makedirs(persistent_directory, exist_ok=True)

        self.text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            chunk_size=2048, chunk_overlap=512
        )
        self.embeddings = OpenAIEmbeddings()

        # 如果本地已有 FAISS 索引，尝试加载
        self.vector_store = None
        index_path = os.path.join(self.persistent_directory, "index.faiss")
        store_path = os.path.join(self.persistent_directory, "index.pkl")
        if os.path.exists(index_path) and os.path.exists(store_path):
            try:
                self.vector_store = FAISS.load_local(
                    self.persistent_directory,
                    self.embeddings,
                    allow_dangerous_deserialization=True,
                )
                ntotal = getattr(getattr(self.vector_store, "index", None), "ntotal", "unknown")
                print(f"成功加载现有 RAG 知识库（FAISS），向量数：{ntotal}")
            except Exception as e:
                print(f"加载现有 FAISS 知识库失败：{e}")
                self.vector_store = None

    def create_database(
        self,
        url_list=None,
        pdf_folder=None,
        json_folder=None,
        py_folder=None,
        txt_folder=None
    ):
        """
        初始化数据库并加载指定的文档（使用 FAISS 持久化）。
        """
        knowledge_docs_list = []

        # 在线文档
        if url_list:
            docs = [WebBaseLoader(url).load() for url in url_list]
            knowledge_docs_list.extend([item for sublist in docs for item in sublist])

        # PDF
        if pdf_folder and os.path.isdir(pdf_folder):
            for file_name in os.listdir(pdf_folder):
                if file_name.lower().endswith(".pdf"):
                    pdf_path = os.path.join(pdf_folder, file_name)
                    try:
                        loader = PyPDFLoader(pdf_path)
                        knowledge_docs_list.extend(loader.load())
                    except Exception as e:
                        print(f"读取 {pdf_path} 失败，跳过。原因：{e}")

        # JSON（按 task_id 拆分）
        if json_folder and os.path.isdir(json_folder):
            for file_name in os.listdir(json_folder):
                if file_name.lower().endswith(".json"):
                    json_path = os.path.join(json_folder, file_name)
                    try:
                        with open(json_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        if isinstance(data, list):
                            for task in data:
                                page_content = json.dumps(task, ensure_ascii=False, indent=2)
                                metadata = {"source": json_path, "task_id": task.get("task_id")}
                                knowledge_docs_list.append(Document(page_content=page_content, metadata=metadata))
                        else:
                            page_content = json.dumps(data, ensure_ascii=False, indent=2)
                            knowledge_docs_list.append(Document(page_content=page_content, metadata={"source": json_path}))
                    except Exception as e:
                        print(f"读取 {json_path} 失败，跳过。原因：{e}")

        # Python
        if py_folder and os.path.isdir(py_folder):
            for file_name in os.listdir(py_folder):
                if file_name.lower().endswith(".py"):
                    py_path = os.path.join(py_folder, file_name)
                    try:
                        knowledge_docs_list.extend(PythonLoader(py_path).load())
                    except Exception as e:
                        print(f"读取 {py_path} 失败，跳过。原因：{e}")

        # TXT
        if txt_folder and os.path.isdir(txt_folder):
            for file_name in os.listdir(txt_folder):
                if file_name.lower().endswith(".txt"):
                    txt_path = os.path.join(txt_folder, file_name)
                    try:
                        knowledge_docs_list.extend(TextLoader(txt_path, encoding="utf-8").load())
                    except Exception:
                        try:
                            knowledge_docs_list.extend(TextLoader(txt_path, encoding="gbk").load())
                        except Exception as e2:
                            print(f"读取 {txt_path} 失败，跳过。原因：{e2}")

        print(f"共加载文档数量：{len(knowledge_docs_list)}")
        for i, doc in enumerate(knowledge_docs_list[:3]):
            print(f"文档 {i + 1} 预览：{doc.page_content[:100]}")

        # 文本拆分（JSON 按 task_id 已经拆过，不再切）
        non_json_docs = [doc for doc in knowledge_docs_list if not doc.metadata.get("task_id")]
        json_task_docs = [doc for doc in knowledge_docs_list if doc.metadata.get("task_id")]

        knowledge_splits = self.text_splitter.split_documents(non_json_docs) + json_task_docs
        print(f"文档切分后总块数：{len(knowledge_splits)}")

        # 写入 FAISS（分批以节省内存）
        batch_size = 32
        for i in range(0, len(knowledge_splits), batch_size):
            batch = knowledge_splits[i:i + batch_size]
            if i == 0 and self.vector_store is None:
                self.vector_store = FAISS.from_documents(
                    documents=batch,
                    embedding=self.embeddings,
                )
            else:
                self.vector_store.add_documents(batch)

        # 持久化到磁盘
        if self.vector_store is not None:
            self.vector_store.save_local(self.persistent_directory)
            ntotal = getattr(getattr(self.vector_store, "index", None), "ntotal", "unknown")
            print(f"RAG 知识库（FAISS）已保存到：{self.persistent_directory}，向量数：{ntotal}")
        else:
            print("未创建 FAISS 向量库，可能是未加载到任何文档。")


# 使用示例
if __name__ == "__main__":
    collection_name1 = "Literature_RAG"
    collection_name2 = "Solution_RAG"
    collection_name3 = "Code_RAG"

    persistent_directory1 = r"C:\NTL-CHAT\地缘环境智能计算平台\RAG_Faiss\Literature_RAG"
    persistent_directory2 = r"C:\NTL-CHAT\地缘环境智能计算平台\RAG_Faiss\Solution_RAG"
    persistent_directory3 = r"C:\NTL-CHAT\地缘环境智能计算平台\RAG_Faiss\Code_RAG"

    rag_db1 = RAGDatabase(persistent_directory1, collection_name1)
    # rag_db2 = RAGDatabase(persistent_directory2, collection_name2)
    # rag_db3 = RAGDatabase(persistent_directory3, collection_name3)

    json_folder = r"C:\NTL-CHAT\tool\RAG\workflow"
    json_folder2 = r"C:\NTL-CHAT\tool\RAG\code_guide\GEE_dataset"
    py_folder = r"C:\NTL-CHAT\tool\RAG\code_guide\Geospatial_Code_GEE"
    txt_folder = r"C:\NTL-CHAT\tool\RAG\code_guide\Geospatial_Code_geopanda_rasterio"
    pdf_folder = r"C:\NTL-CHAT\tool\RAG\文献查找\综述"

    rag_db1.create_database(pdf_folder=pdf_folder)
    # rag_db2.create_database(json_folder=json_folder)
    # rag_db3.create_database(py_folder=py_folder, txt_folder=txt_folder, json_folder=json_folder2)

    print("RAG 数据库创建完成。")

