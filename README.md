# Description of one pass

Development and implementation of intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. (WP9)

This README is a working document that will contain the sample one-pass algorithms that will be written in Python3. 

# One-pass method

$x_1 \qquad \qquad \qquad \qquad x_2 \qquad \qquad \qquad \qquad x_3 \qquad ... \qquad \qquad x_n$ 
$\downarrow \qquad \qquad \qquad \qquad \; \; \downarrow \qquad \qquad \qquad \qquad \; \; \downarrow \qquad \qquad \qquad \quad \;\downarrow$
$S_1 = g(x_1) \rightarrow S_2 = g(S_1, x_2) \rightarrow S_3 = g(S_2, x_3) \rightarrow S_n = g(S_{n-1}, x_n)$

Let $X_n = \{x_1, x_2, ..., x_n\}$ represent the data set, then $S_n = f(X_n) = g(S_{n-1}, x_n)$ 
Where $f$ is a function that acts on the  whole data set and computes the summary $S_n$ at once with all values, $g$ is a 'one pass' function that updates a previous summary $S_{n-1}$ with a new value $x_n$. The summaries $S_n$ require less memory than the full dataset $X_n$. 
# Algorithms

## min / max 

$g(S_{n-1}, x_n) = g(min_{n-1}, x_n) 
\newline \qquad \qquad \quad = if (x_n < min_{n-1})
\newline \qquad \qquad \qquad \quad  min_{n-1} = x_n$

$g(S_{n-1}, x_n) = g(max_{n-1}, x_n) 
\newline \qquad \qquad \quad = if (x_n > max_{n-1})
\newline \qquad \qquad \qquad \quad  max_{n-1} = x_n$

<u> memory requirements: </u>
One spatial grid of double (float64) the same size as the original data to store $min_{n-1}$  or $max_{n-1}$

## mean 

$\bar{x}_n = g(S_{n-1}, x_n) = g(\bar{x}_{n-1}, x_n) = \frac{n-1}{n}\bar{x}_{n-1} + \frac{x_n}{n} = \bar{x}_{n-1} + \frac{x_n - \bar{x}_{n-1}}{n}$ 

<u> memory requirements: </u>
- One spatial grid of double (float64) the same size as the original data to store $\bar{x}_{n-1}$  
- one int to store $n$ 

## threshold exceedance 

$g(S_{n-1}, x_n) = g(exc_{n-1}, x_n) 
\newline \qquad \qquad \quad = if (x_n > threshold)
\newline \qquad \qquad \qquad \quad  exc_{n-1} = exc_{n-1} + 1$

<u> memory requirements: </u>
- one int to store $exc_{n-1}$ 

## Variance 
(shifted Welford's algorithm for the unbiased sample variance)
Requires updating two estimates iteratively. Let $M_{2,n} = \sum_{i = 1}^{n}(x_i - \bar{x}_n)^2$ then: 
$M_{2,n} = g(S_{n-1}, x_n) = g(M_{2,n-1}, x_n) =M_{2,n-1} + (x_i - \bar{x}_{n-1})(x_i - \bar{x}_n)$ and $\bar{x}_n$ is given by the algorithm of the mean shown above. At the end of the iterative process (when the last value is given):
$var(X_n) = \frac{M_{2,n}}{n-1}$
If a rough estimate of the mean is avaliable (needs to be a constant value), subtract at each step, as $var(X_n - k) = var(X_n)$ but subtracting mean can reduce large values that produce numerical instability. E.g. for sea level pressure, data is around 100000 Pa. If doing this, calculate $x' = x - k$ at the beginning of each step before computing algorithm.

Numerical stability warning. For large $n$, $M_{2,n}$ can become larger than max float value and produce inaccurate results. In such cases, one could divide by the current $n$ and store an intermediate variance value, to be cobined later with other chunks using Chan's formula for the general case of combining the variance of two chunks of data. An alternative is to explore Kahan summation.


## histogram
Decide the bin size of the histogram based on the precision of your variable. e.g. for wind speed, storing more than one figure is meaningless for users. Considering a range of wind speed from 0 to 100 m/s every 0.1 ms results in storing counts for a maximum of 1000 values.

<u> memory requirements: </u>
- k integers to store occurrences for each bin
- k doubles to store the bin centers (this could be derived from bin width as well
saving a bit of space)
Memory savings: this method starts to save space whenever values repeat themselves, therefore as soon as counts for each bin start to grow larger than 1 we save space.

## wind power CF
Compute a histogram with previous algorithm. For each bin, look in the power curve for the power output or CF value. Weight all CF values by the frequency of each bin.

## Algorithm for percentiles
Compute the histogram above. Ensure a minimum number of bins in the histogram to get accurate results. Derive percentiles from the histogram by accumulating the histogram frequencies.

## wind direction standard deviation
Difficulty: values close to 0/360 should not average to 180.Use Yamartino method.
