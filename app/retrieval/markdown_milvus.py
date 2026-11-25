"""Versioned Milvus search indexes for Markdown passages."""
from hashlib import sha256
import json
from threading import Lock

from pymilvus import DataType, Function, FunctionType, MilvusClient
from pymilvus.exceptions import MilvusException


FIELDS = {"bm25": ("bm25", "BM25"), "dense": ("dense", "IP"), "sparse": ("sparse", "IP")}


def sparse_vector(vector):
    return {index: value for index, value in zip(vector["indices"], vector["values"], strict=True) if value}


class MarkdownMilvus:
    def __init__(self, settings, embed, embedding_model):
        self.client = MilvusClient(uri=settings.milvus_uri, token=settings.milvus_token or "", timeout=15)
        self.dim = settings.embedding_dim
        self.embed = embed
        self.embedding_model = embedding_model
        self.collection = None
        self.lock = Lock()

    def prepare(self, passages):
        with self.lock:
            if self.collection is not None:
                return self.collection
            key = json.dumps(["markdown-v1", self.embedding_model(), self.dim, passages],
                             ensure_ascii=False, separators=(",", ":"))
            name = "markdown_" + sha256(key.encode()).hexdigest()[:32]
            try:
                if not self.client.has_collection(name):
                    self._create(name)
                self.client.load_collection(name)
                count = self.client.query(name, output_fields=["count(*)"], consistency_level="Strong")[0]["count(*)"]
                if count != len(passages):
                    vectors = self.embed([text for _, text in passages], "dense_sparse")
                    dense, sparse = vectors["dense"], vectors["sparse"]
                    if len(dense) != len(passages) or len(sparse) != len(passages):
                        raise RuntimeError("Embedding service returned an incomplete document")
                    for start in range(0, len(passages), 100):
                        stop = min(start + 100, len(passages))
                        rows = [{"id": i, "text": passages[i][1], "dense": dense[i],
                                 "sparse": sparse_vector(sparse[i])} for i in range(start, stop)]
                        self.client.upsert(name, rows)
                    self.client.flush(name)
            except MilvusException as exc:
                raise RuntimeError(f"Markdown Milvus index unavailable: {exc.message}") from exc
            self.collection = name
            return name

    def _create(self, name):
        schema = self.client.create_schema(auto_id=False, enable_dynamic_field=False)
        schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
        schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=16384,
                         enable_analyzer=True, analyzer_params={"tokenizer": "jieba", "filter": ["cnalphanumonly"]})
        schema.add_field(field_name="dense", datatype=DataType.FLOAT_VECTOR, dim=self.dim)
        for field in ("bm25", "sparse"):
            schema.add_field(field_name=field, datatype=DataType.SPARSE_FLOAT_VECTOR)
        schema.add_function(Function(name="markdown_bm25", function_type=FunctionType.BM25,
                                     input_field_names=["text"], output_field_names=["bm25"]))
        indexes = self.client.prepare_index_params()
        indexes.add_index(field_name="dense", index_type="FLAT", metric_type="IP")
        for field, metric in (FIELDS["bm25"], FIELDS["sparse"]):
            indexes.add_index(field_name=field, index_type="SPARSE_INVERTED_INDEX", metric_type=metric)
        self.client.create_collection(name, schema=schema, index_params=indexes, consistency_level="Strong")

    def search(self, passages, query, mode):
        field, metric = FIELDS[mode]
        name = self.prepare(passages)
        value = query if mode == "bm25" else self.embed([query], mode)[0]
        if mode == "sparse":
            value = sparse_vector(value)
            if not value:
                return []
        try:
            hits = self.client.search(name, data=[value], anns_field=field, limit=100,
                                      search_params={"metric_type": metric})[0]
        except MilvusException as exc:
            raise RuntimeError(f"Markdown Milvus search failed: {exc.message}") from exc
        return [hit["id"] for hit in hits]
