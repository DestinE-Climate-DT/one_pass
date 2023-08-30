Introduction
===============

Background 
---------------

Currently, impact modelling and downstream applications use pre-existing climate model output archives (e.g., CORDEX and CMIP archives) to create information for climate adaptation policies and socio-economic decisions. Climate DT proposes a novel way of running user applications close to the Earth system models by streaming output variables directly to applications instead of storing a limited set of essential climate variables on disk. This novel way of post-processing model outputs as they become available allows an analysis of high-frequency and high-resolution variables that would be too costly to store on disk at the resolutions envisaged by climate DT. 

The streaming of climate model outputs will enable applications to access a larger set of variables at unprecedented resolutions for downstream impact modelling at global scales. This approach goes beyond the classical paradigm where adaptation studies have to use variables from archives with only a limited predefined number of variables and at coarse resolutions. However, this streaming of the output variables creates the one-pass problem: the algorithms that compute summaries, diagnostics or derived quantities do not have access anymore to the whole time series, but just receive the values incrementally every time that the model outputs new time steps. The specific computations required by downstream applications can cover a wide range of topics from computing time aggregations or distribution percentiles, bias-adjusting, deriving established climate indices or computing sectoral indicators. Any downstream application that is part of the Climate DT workflow can make use of the one_pass algorithm library, detailed in this documentation, to help them run in streaming mode.

One-pass method
-------------------

One-pass algorithms (also known as single-pass algorithms) are streaming algorithms which read the data input only one chunk at a time. This is done by handelling items in order (without buffering); reading and processing a block of data and moving the result into an output buffer before before moving onto the next block. This is described mathematically by: 

.. math::
 x_1 \quad \qquad \qquad \qquad x_2 \qquad \qquad \qquad \quad x_3 \qquad ... \qquad \quad x_n \qquad
       
 \downarrow \quad \qquad \qquad \qquad  \; \; \downarrow \quad \qquad \qquad \qquad \; \; \downarrow \; \; \quad \qquad \qquad \quad \;\downarrow \qquad
    
 S_1 = g(x_1) \rightarrow S_2 = g(S_1, x_2) \rightarrow S_3 = g(S_2, x_3) \rightarrow S_n = g(S_{n-1}, x_n).

 
If we let :math:`X_n = \{x_1, x_2, ..., x_n\}` represent the data set at time :math:`n`, then :math:`S_n = f(X_n) = g(S_{n-1}, x_n)`, where :math:`f` is a function that acts on the  whole data set and computes the summary :math:`S_n` in one go using all values and :math:`g` is a 'one_pass' function that updates a previous summary :math:`S_{n-1}` with a new value :math:`x_n`. The summaries :math:`S_n` require less memory than the full dataset :math:`X_n`.
