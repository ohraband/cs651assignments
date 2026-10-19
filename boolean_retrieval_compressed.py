# boolean_retrieval_compressed.py

import argparse
import pickle

# Add additional imports if needed.

def vint_decode_one(buf, i):
    # returns (value, next_index)
    shift = 0
    n = 0
    while True:
        b = buf[i]
        i += 1
        n |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return n, i

# BEGIN OF YOUR CODE
def decode_postings(ba):
    postings = []
    i = 0
    df, i = vint_decode_one(ba, i)

    last_docid = 0
    for _ in range(df):
        delta, i = vint_decode_one(ba, i)
        docid = last_docid + delta
        tf, i = vint_decode_one(ba, i)
        postings.append((docid, tf))
        last_docid = docid
    return postings


def load_index(index_file):
    with open(index_file, 'rb') as f:
        compressed_index = pickle.load(f)

    index = {}
    for term, ba in compressed_index.items():
        postings = decode_postings(ba)
        index[term] = sorted(docid for docid, tf in postings)
    return index


def run_query(index_file, corpus_file, query):
    index = load_index(index_file)

    tokens = query.strip().lower().split()
    result_set = None
    op = None

    for tok in tokens:
        if tok in ("and", "or"):
            op = tok
        else:
            postings = set(index.get(tok, []))
            if result_set is None:
                result_set = postings
            else:
                if op == "and":
                    result_set &= postings
                elif op == "or":
                    result_set |= postings
            op = None

    if not result_set:
        return

    results = sorted(result_set)

    corpus_dict = {}
    with open(corpus_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            parts = line.rstrip("\n").split("\t", 2)
            if len(parts) != 3:
                continue
            docid = int(parts[0])
            if docid in result_set:
                corpus_dict[docid] = (parts[1], parts[2])


    for docid in results:
        title, sentence = corpus_dict[docid]
        sentence = sentence.replace("\n", " ")
        sentence_trimmed = sentence[:100] + "..." if len(sentence) > 100 else sentence
        print(f"{docid:8} {title:72} {sentence_trimmed}")

# END OF YOUR CODE

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-index', required=True)
    ap.add_argument('-corpus', required=True)
    ap.add_argument('-query', required=True)
    args = ap.parse_args()

    run_query(args.index, args.corpus, args.query)


if __name__ == '__main__':
    main()
