# Drift detectors

*DriftPipe* defines a stage for detecting drift on the inference data given the baseline statistics. There are two types of pipes: *StatsDriftPipe* and *WindowDriftPipe*. The first attempts to detect drift on the inference data based on previously computed statistics (e.g feature distributions), while the second does the same directly from the instances. Therefore, *StatsDriftPipe* can be attached to the StatsPipe while *WindowDriftPipe* is attached directly to the *WindowPipe*.

> *NOTE*: Currently, pipes can be preceded by only one pipe. Support for merging multiple pipes (e.g *WindowPipe*) can enable more efficient drift detection algorithms.

Using statistics:

- **WassersteinDetector**: Computes the Wasserstein distance (or Earth Mover's Distance) between the inference features distributions and the ones provided by the baseline.
- **KullbackLeiblerDetector**: Computes the Kullback-Leibler divergence between the inference features distributions and the ones provided by the baseline.
- **JensenShannonDetector**: Computes the Jensen-Shannon divergence between the inference features distributions and the ones provided by the baseline.

Using features:

- Pending

| Name                 | Implemented | Roadmap |
|----------------------|-------------|---------|
| Wasserstein distance | X           | X
| Kullback-Leibler div.| X           | X
| Jensen-Shannon div.  | X           | X
| Kolgomorov-Smirnov   |             |
| Maximum Mean Discrepancy |             |
| ADWIN / ADWIN2       |             |
| Page Hinkley         |             |
| DDM                  |             |
| EDDM                 |             |
| FHDDM                |             |
| FHDDMS               |             |
| HDDM                 |             |
| MDDM                 |             |
| RDDM                 |             |
| Cusum / Cusum2       |             |
