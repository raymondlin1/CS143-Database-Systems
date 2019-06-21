#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import json
import sys
from subprocess import Popen, PIPE, STDOUT

__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:
    
    # Replace new lines and tab characters with a single space
    text = text.replace(r'\n', ' ')
    text = text.replace(r'\t', ' ')
    
    # Remove both types URL formats
    text = re.sub(r'(\[[^]]*])(\(https?://[^)]*\))', r'\1', text)
    text = re.sub(r'https?://[^ ]*', '', text)
    text = re.sub(r'www\.[^ ]*', '', text)
    
    # Remove subreddit/user links encoded with Markup links
    text = re.sub(r'(\[[^]]*])(\(\/[^/]*\/[^ /]*\/?\))', r'\1', text)
    
    # The punctuation marks .! ? , : ; will not be removed (according to tester)
    
    # Remove all valid punctuation
    text = re.sub(r'[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+[\\"#$%&\'()*+/<=>@[\]^_`{|}~]+[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+', ' ', text)
    text = re.sub(r'[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+[\\"#$%&\'()*+/<=>@[\]^_`{|}~]+', ' ', text)
    text = re.sub(r'[\\"#$%&\'()*+/<=>@[\]^_`{|}~]+[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+', ' ', text)
    text = re.sub(r'([.!?,;:])([\\"#$%&\'()*+/<=>@[\]^_`{|}~])+([.!?,;:])', r'\1\3', text)
    text = re.sub(r'([.!?,;:])([\\"#$%&\'()*+/<=>@[\]^_`{|}~])', r'\1', text)
    text = re.sub(r'([\\"#$%&\'()*+/<=>@[\]^_`{|}~])+([.!?,;:])', r'\2', text)

    # - must be removed separately or else I get weird bug
    text = re.sub(r'[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+-[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+', ' ', text)
    text = re.sub(r'[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+-', ' ', text)
    text = re.sub(r'-[\\"#$%&\'()*+/<=>@[\]^_`{|}~ ]+', ' ', text)
    text = re.sub(r'([.!?,;:])(-)+([.!?,;:])', r'\1\3', text)
    text = re.sub(r'([.!?,;:])(-)+', r'\1', text)
    text = re.sub(r'(-)+([.!?,;:])', r'\2', text)

    # Convert all text to lowercase
    text = text.lower()
    
    # Separate external punctuation into separate tokens
    text = re.sub(r'([.!?,;:])([.!?,;: ])', r' \1 \2 ', text)
    text = re.sub(r'([.!?,;: ])([.!?,;:])', r' \1 \2 ', text)

    # Split text on a single space
    text = text.split(' ')
    
    # Remove empty tokens
    text = list(filter(None, text))
    
    # Format the parsed text, unigrams, bigrams, and trigrams
    punc = '.!?,;:'
    text_len = len(text)
    parsed_text = ''
    unigrams = ''
    bigrams = ''
    trigrams = ''
    
    for k in range(text_len):
        parsed_text += text[k] + ' '
    parsed_text = parsed_text[:-1]
    
    for k in range(text_len):
        if text[k] not in punc:
            unigrams += text[k] + ' '
    unigrams = unigrams[:-1]
    
    for k in range(text_len-1):
        if text[k] not in punc and text[k+1] not in punc:
            bigrams += text[k] + '_' + text[k+1] + ' '
    bigrams = bigrams[:-1]

    for k in range(text_len-2):
        if text[k] not in punc and text[k+1] not in punc and text[k+2] not in punc:
            trigrams += text[k] + '_' + text[k+1] + '_' + text[k+2] + ' '
    trigrams = trigrams[:-1]

    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.
    if len(sys.argv) != 2:
        print("USAGE: ./cleantext.py [string: filename]")
    else:
        json_file = sys.argv[1]
        try:
            open(json_file, "r")
        except OSError as e:
            print("Error: file not found")
            sys.exit()

        print("Success! File named {} found".format(json_file))
        fp = Popen("jq '.body' {}".format(json_file), shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        output = fp.stdout.read()
        split_val = str.encode('\"\n\"')
        comments_byte_strings = output.split(split_val)

        converter = lambda x: x.decode("utf-8")
        comments_strings = list(map( converter, comments_byte_strings))
        for comment in comments_strings:
            print("Original: {}".format(comment))
            parsed_tuple = sanitize(comment)

            print("Parsed: {}\n".format(parsed_tuple[0]))
            print("Unigram: {}\n".format(parsed_tuple[1]))
            print("Bigram: {}\n".format(parsed_tuple[2]))
            print("Trigram: {}\n".format(parsed_tuple[3]))

    exit(1)

