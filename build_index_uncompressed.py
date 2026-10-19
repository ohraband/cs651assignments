## build_index_uncompressed.py

import argparse
import re
from collections import Counter
from pyspark import SparkConf, SparkContext

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
        return int(parts[0]), parts[1]
    else:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-input', required=True, help='Path to the input corpus file.')
    ap.add_argument('-output', required=True, help='Path to the output index directory.')
    args = ap.parse_args()

    conf = SparkConf().setAppName('build_index_uncompressed')
    sc = SparkContext(conf=conf)

    # Read content in the file (docid, text)
    rdd = sc.textFile(args.input)

    # BEGIN OF YOUR CODE

    docs = rdd.map(parse_line)

    term_doc_tf = docs.flatMap(lambda doc: [
        (term, (doc[0], count))
        for term, count in Counter(tokenize(doc[1])).items()
    ])

    grouped_postings = term_doc_tf.groupByKey()

 
    def format_postings(term_postings):
        term, postings = term_postings
        sorted_postings = sorted(list(postings), key=lambda x: x[0])
        df = len(sorted_postings)
        
        postings_list_str = ' '.join(
            [f"{docid}:{tf}" for docid, tf in sorted_postings]
        )

        return f"{term}\t{df} {postings_list_str}"

    postings_lists = grouped_postings.map(format_postings)

    # END OF YOUR CODE

    postings_lists.saveAsTextFile(args.output)
    sc.stop()


if __name__ == '__main__':
    main()