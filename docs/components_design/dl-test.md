We analyze DL unit test results to establish actionable cluster health 
thresholds. Our primary objective is to verify if any nodes/GPUs have notably 
bad performance compared to the whole cluster while also tracking long-term 
hardware degradation and removing these nodes from the cluster when found.

Node health needs to be tracked to determine if a node should be 
tested due to either:
*   **Exceptionally bad test results** (many bad test results, or tests 
indicating it is a straggler).
*   **Decline in node health** (e.g., a number of GPU test shows "Excellent" or 
"Normal" consistently, then consitently degrades to "bad").

To do this, we split DL unit test metrics into two categories: **Numerical** 
and **Temporal**.

# Metrics

## Numerical Metrics
These specific metrics are either good or bad since faulty numerical results 
will invalidate training. Most layers will give the exact same value, but some 
(flash attention for example) can yield different results from test to test due 
to the non-deterministic nature of the algorithm on the hardware. These should 
be as close to baseline as possible.

*   **Types of numerical metrics:**
    *   **Norm_output:** L2 norm from the forward pass.
    *   **Weight terms:** L2 norm of weight terms after the backward pass.
    *   **Bias terms:** L2 norm of bias terms from the backward pass.
*   **Baseline:** The most common result over all GPUs testded.
*   **Assumption:** We assume that most GPUs are behaving normally for the 
numerical results.
*   **Threshold:** 1% or more deviation from baseline for FP32, FP16, and BF16 
results in a **bad** numerical result.

## Temporal Metrics
These metrics represent GPU and system performance. Becuase performance can be 
affect by issue unrealted to the actual hardware, we don't set a hard fail 
criteria for any of the metrics. Instead we look at overall GPU performance 
across many tests, and overall node performance.

### Standard Metrics
*   **Types of temporal metrics:**
    *   **CPU times:** Forward Pass, Backward Pass.
    *   **GPU times:** Forward Pass, Backward Pass.
*   **Baseline:** The baseline is set with the **median**. We've seen many 
distributions (multi-modal, discrete, etc.), and the median works better than 
the mean for many, especially ones with outliers.
*   **Ranges & Thresholds:**
    *   **Excellent:** Below the baseline.
    *   **Normal:** From the baseline to the tail threshold.
    *   **Bad:** The upper tail of the distribution with an IQR of 95 and above 
    is used to find the upper tail of the distribution.

### Overlap Metrics
Overlap tests have a slightly different interpretation while still falling 
under temporal. These test are meant to expose any issues where overlap may 
cause issues or stragglers.

*   **Types of overlap metrics:**
    *   **Means:** Collective mean (coll_mean) or layer mean (layer_mean). For 
    example, coll_mean is the mean time in ms of the collective operation 
    executed for the given number of iterations while a specific layer is 
    executing at the same time.
    *   **Standard deviations:** coll_stdev or layer_stdev is the standard 
    deviation of the collective or layer mean.

# Evaluation FLow

1. Data from many nodes/GPUs on the cluster are used to create the **baseline 
distribution** where a target baseline for a metric, along with the "bad" and 
"very bad" thresholds, are determined.
    * **Configuration data:** Data needs to be separated into sets based on 
    **GPU**, **CUDA version**, **PyTorch version**
    * **Define Baseline Distribution**: The number of samples for 5% confidence 
    for all non-overlap tests is around 100 or less. Our rule of thumb is to 
    get at least 100 separate GPUs worth of data for the same tests, metric, 
    and configuration.
    * **Determine Target Baselines/Exceptional Range:** For each test and 
    metric, determine every target baseline. Results below the baseline are
    considered "Exceptional"
    * **Determine "Normal/Bad Cutoff:** For each test and metric, use the 
    95th IQR to determine the cutoff where the "Bad" results start. Anything 
    from the baseline up to the bad cutoff is considered "Normal." 
2. Cordoned node runs DL unit test as part of C-Val.
    * Each metric of for each test on each GPU of node categorized.
    * Nodes/GPUs with a high number of "bad" results or shows straggler issues 
    will be cordoned.
3. New C-val data from DL Unit Test that is not considered an outlier can be 
incorporated into the baseline distribution and measurements.
    * This will need to periodically occur in batches otherwise a single new 
    point can shift the baselines distributions.
    * Baseline measurements need to be tracked to see if there are any overall 
    trends.
    * Baseline distribution data sets depend on GPU, CUDA version, 
    PyTorch version.

## GPU and Node Evaluation Scoring

To evaluate the overall health of the hardware, results are scored and summed both per GPU and per node based on the categorizations above.

### Temporal Non-Overlapped Metrics
For temporal non-overlapped metrics, points are assigned as follows:
*   **Excellent:** 0 score
*   **Normal:** 1 score
*   **Bad:** 3 score

Based on the total summed score per GPU and per node:
*   **Excellent condition:** Total score is lower than the total number of these tests.
*   **Normal / Good condition:** Total score is at least the quantity of tests up to a specified threshold. 
*   **Bad / Very Bad condition:** Exact thresholds are still to be determined, but these will apply to high-scoring GPUs and nodes.

### Numerical Metrics
*   **Automatic Failure:** Any score above the total number of numerical tests indicates an automatically **bad node** and must be checked immediately.

### Overlap Tests
*   Scoring and evaluation thresholds for overlap tests will need to be studied more.