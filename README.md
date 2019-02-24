# Overview

This program extracts the top 3 sentneces that summarize the article using MapReduce. Ranking of the setences is computed using TF.IDF scores; TF.IDF values are computed for each unigram, the top 5 scores per sentecne are summed, giving the Sentence-TF.IDF value.

Input Data
The format of the input data for PA2 is identical to PA1. Extraneous periods were removed to increase accuracy of sentence parsing.
