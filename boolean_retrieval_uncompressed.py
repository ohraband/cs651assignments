# boolean_retrieval_uncompressed.py

import argparse
import os

# BEGIN OF YOUR CODE

def load_index(index_dir):
    index = {}
    for fname in os.listdir(index_dir):
        if not fname.startswith("part-"):
            continue
        path = os.path.join(index_dir, fname)
        if os.path.isfile(path):
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    line = line.strip()
                    if not line or "\t" not in line:
                        continue
                    term, postings_str = line.split("\t", 1)

                    parts = postings_str.strip().split()
                    if len(parts) < 2:
                        continue

                    docids = []
                    for x in parts[1:]:
                        docid = int(x.split(":")[0])
                        docids.append(docid)
                    index[term] = sorted(docids)
    return index


def run_query(index_dir, corpus_file, query):
    index = load_index(index_dir)

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
                    result_set = result_set & postings
                elif op == "or":
                    result_set = result_set | postings
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


    # You'll have to implement this function:
    run_query(args.index, args.corpus, args.query)


if __name__ == '__main__':
    main()
