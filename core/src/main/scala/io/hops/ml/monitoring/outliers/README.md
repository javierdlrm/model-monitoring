# Outlier detectors

Outlier pipes define a stage for detecting outliers on the inference data given the baseline statistics. There are two types of pipes: *StatsOutlierPipe* and *WindowOutlierPipe*. The first finds outliers from previously computed statistics, while the second does the same directly from instances. Therefore, *StatsOutlierPipe* can be attached to the *StatsPipe* receiving the statistics as input, while *WindowOutlierPipe* is attached directly to the WindowPipe.

Using statistics:
- **DescriptiveStatsDetector**: Threshold-based statistics comparison against the baseline.

Using instances:
- **AEDetector** (not implemented yet): Detects changes in the instances using a pre-trained variational autoencoder.
- **MahalanobisDetector** (not implemented yet): Detects changes in the instances comparing the mahalanobis distance with a given threshold.


| Name                 | Implemented | Roadmap |
|----------------------|-------------|---------|
| Descriptive          | X           | X
| Mahalanobis distance |             |
| AE                   |             |
| VAE                  |             |
| AEGMM                |             |
| VAEGMM               |             |
| Isolation forest     |             |
| Prophet              |             |
| Spectral residual    |             |
| Seq2Seq              |             |
