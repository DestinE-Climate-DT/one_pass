## Description of one pass

Development and implementation of intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. (WP9)

This README is a working document that will contain the sample one-pass algorithms that will be written in Python3. 

## Algorithms

# min / max 

    \g(S_{n-1}, x_n) = g(min_{n-1}, x_n) = if else(x_n < min_{n-1}, x_n, min_{n-1})
