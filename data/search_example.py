import dsp
import json
import numpy as np
from .load_data import *

def knn(train, ids, **knn_args) :
    from dsp.ann_utils import create_faiss_index


    vectorizer = dsp.vectorizer.SentenceTransformersVectorizer()
    all_vectors = vectorizer(train).astype(np.float32)

    index = create_faiss_index(
        emb_dim=all_vectors.shape[1], n_objects=len(train), **knn_args
    )
    index.train(all_vectors)
    index.add(all_vectors)

    def inner_knn_search(inp_example, k):
        inp_example_vector = vectorizer(inp_example)
        inp_example_vector = inp_example_vector.reshape(1, -1)
        scores_idx, nearest_samples_idxs = index.search(inp_example_vector, k)
        sampled_id = [ids[cur_idx] for cur_idx in nearest_samples_idxs[0]]
        sampled_question = [train[cur_idx] for cur_idx in nearest_samples_idxs[0]]
        reranked_candidates = [(q, q_id, score) for q, q_id, score in zip(sampled_question, sampled_id, scores_idx[0])]
        return reranked_candidates

    return inner_knn_search   


def get_example(train, ids, question, n_shot):
    knn_func = knn(train, ids)
    reranked_candidates = knn_func(question, n_shot)
    return reranked_candidates

            