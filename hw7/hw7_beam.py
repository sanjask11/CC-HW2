#!/usr/bin/env python3

import argparse
import re
import time
import os

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

LINK_RE = re.compile(r'<a\s+HREF="([^"]+)"', re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WORD_RE = re.compile(r"[A-Za-z0-9']+")


def read_file(readable_file):
    path = readable_file.metadata.path
    text = readable_file.read_utf8()
    return path, text


def normalize_link(target):
    base = os.path.basename(target.strip())
    if re.fullmatch(r"\d+\.html", base):
        return base
    return None


def extract_links(html):
    links = []
    for raw in LINK_RE.findall(html):
        name = normalize_link(raw)
        if name:
            links.append(name)
    return links


def outgoing_links(record):
    path, html = record
    src = os.path.basename(path)
    links = extract_links(html)
    yield src, len(links)


def incoming_links(record):
    _, html = record
    links = extract_links(html)
    for dst in links:
        yield dst, 1


def html_words(html):
    text = TAG_RE.sub(" ", html)
    words = WORD_RE.findall(text.lower())
    return words


def bigrams(record):
    _, html = record
    words = html_words(html)
    for i in range(len(words) - 1):
        yield f"{words[i]} {words[i+1]}", 1


def format_top(records):
    ordered = sorted(records, key=lambda x: (-x[1], x[0]))
    for r in ordered:
        yield f"{r[0]}\t{r[1]}"


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--topk", type=int, default=5)

    args, beam_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    start = time.time()

    with beam.Pipeline(options=pipeline_options) as p:

        files = (
            p
            | "MatchFiles" >> fileio.MatchFiles(args.input)
            | "ReadMatches" >> fileio.ReadMatches()
            | "ReadFileText" >> beam.Map(read_file)
        )

        
        (
            files
            | "IncomingEdges" >> beam.FlatMap(incoming_links)
            | "SumIncoming" >> beam.CombinePerKey(sum)
            | "TopIncoming" >> beam.combiners.Top.Of(args.topk, key=lambda x: x[1])
            | "FormatIncoming" >> beam.FlatMap(format_top)
            | "WriteIncoming" >> WriteToText(
                f"{args.output}/top_incoming",
                file_name_suffix=".txt",
                num_shards=1,
            )
        )

        
        (
            files
            | "OutgoingLinks" >> beam.FlatMap(outgoing_links)
            | "TopOutgoing" >> beam.combiners.Top.Of(args.topk, key=lambda x: x[1])
            | "FormatOutgoing" >> beam.FlatMap(format_top)
            | "WriteOutgoing" >> WriteToText(
                f"{args.output}/top_outgoing",
                file_name_suffix=".txt",
                num_shards=1,
            )
        )

        
        (
            files
            | "Bigrams" >> beam.FlatMap(bigrams)
            | "SumBigrams" >> beam.CombinePerKey(sum)
            | "TopBigrams" >> beam.combiners.Top.Of(args.topk, key=lambda x: x[1])
            | "FormatBigrams" >> beam.FlatMap(format_top)
            | "WriteBigrams" >> WriteToText(
                f"{args.output}/top_bigrams",
                file_name_suffix=".txt",
                num_shards=1,
            )
        )

    end = time.time()

    print("INPUT:", args.input)
    print("OUTPUT:", args.output)
    print("TOPK:", args.topk)
    print("TOTAL_RUNTIME_SECONDS:", round(end - start, 3))


if __name__ == "__main__":
    main()