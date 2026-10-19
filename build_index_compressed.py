# build_index_compressed.py

import argparse
import re
import pickle
from collections import Counter
from pyspark import SparkConf, SparkContext

# Add additional imports if needed.

# Use this function to tokenize text.
def tokenize(line):
    TOKEN_RE = re.compile(r'(^[^a-z]+|[^a-z]+$)')
    out = []
    for tok in line.split():
        w = TOKEN_RE.sub('', tok.lower())
        if w:
            out.append(w)
    return out

# Each input line is "docid<TAB>text".
# Use this function to parse.
def parse_line(line):
    parts = line.split("\t", 1)
    if len(parts) == 2:
        try:
            return int(parts[0]), parts[1]
        except ValueError:
            return None
    else:
        return None


def vint_encode(n):
    # variable int encoding into a byte array. 
    # We’ll use a standard 7-bit continuation encoding, which we will also decode on the reader side.
    b = bytearray()
    while True:
        to_write = n & 0x7F
        n >>= 7
        if n:
            b.append(to_write | 0x80)
        else:
            b.append(to_write)
            break
    return bytes(b)

# BEGIN OF YOUR CODE
# Write your code below (if needed).


# END OF YOUR CODE


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-input', required=True)
    ap.add_argument('-output', required=True)
    args = ap.parse_args()

    conf = SparkConf().setAppName('build_index_compressed')
    sc = SparkContext(conf=conf)

    # Read content in the file (docid, text)
    rdd = sc.textFile(args.input)

    # BEGIN OF YOUR CODE
    parsed = rdd.map(parse_line)

    term_doc_tf = parsed.flatMap(lambda doc: [
            (term, (doc[0], tf))
            for term, tf in Counter(tokenize(doc[1])).items()
        ])

    grouped = term_doc_tf.groupByKey()


    def encode_postings(term, postings):
        postings = list(postings)
        postings.sort(key=lambda x: x[0])

        df = len(postings)
        ba = bytearray()
        ba.extend(vint_encode(df))

        last_docid = 0
        for docid, tf in postings:
            delta = docid - last_docid
            ba.extend(vint_encode(delta))
            ba.extend(vint_encode(tf))
            last_docid = docid
        return term, ba

    compressed_index = grouped.map(lambda x: encode_postings(x[0], x[1])).collectAsMap()

    with open(args.output, 'wb') as f:
        pickle.dump(compressed_index, f, protocol=pickle.HIGHEST_PROTOCOL)

    # END OF YOUR CODE

    sc.stop()


if __name__ == '__main__':
    main()
