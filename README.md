# Model Monitoring for Hopsworks

A framework for monitoring ML models on Hopsworks.

## Content

- Components
- Monitor
- Windows
- Statistics
- Outliers
- Drift
- Extensibility
- Examples

## Components

Following the semantics of flows of data canalized by different attached streaming queries, the main components of the framework are:

- **Pipeline**: Bunch of joint pipes. It involves a set of pipes and joints (probably shared with other pipelines) that specify a whole processing sequence from source to sink.
- **PipeJoint**: Interface to connect two pipes. It specifies the requirements for connecting to this pipe. It must be implemented by any pipe aiming to be attached.
- **Pipe**: Stage of multiple processing tasks under the same scope (i.e reading from a source, computing statistics, outlier detection,...). Pipes can be shared between different pipelines.

## Monitor

*MonitorPipe* is the first pipe after reading from a source.

## Windows

*WindowPipe* defines the window settings for the pipeline. It requires the name of the timestamp column and a WindowsSettings object including duration, slide duration and watermark delay.

> *NOTE*: Other settings such as min size (i.e count-based) or dynamic windows might be interesting alternatives.

## Statistics

*StatsPipe* defines a stage for computing statistics that can be written into a sink or used in following joint pipes.
Each statistic has a *StatDefinition*, where specific parameters can be set (e.g sample or population), and a *StatAggregator*.

There are three types of *StatAggregator*: *Simple*, *Compound* and *Multiple*. *Simple* and *Compound* statistics are computed feature-wise, while *Multiple* statistics are computed for all available features. The first refers to simple statistics that can be computed linearly (i.e max, min, ...); the second refers to statistics that requires one or more simple statistics be available (i.e mean, average, standard deviation, ...); The third refers to statistics that are computed across all available features and might require none, one or more simple or compound statistics (i.e covariance, correlation,...).

Baseline statistics can be specified as a map of feature -> stat -> value or by name of the training set in the feature store.

## Outliers

*OutlierPipe* defines a stage for detecting outliers on the inference data given the baseline statistics. There are two types of pipes: *StatsOutlierPipe* and *WindowOutlierPipe*. The first finds outliers from previously computed statistics, while the second does the same directly from instances. Therefore, *StatsOutlierPipe* can be attached to the *StatsPipe* receiving the statistics as input, while *WindowOutlierPipe* is attached directly to the WindowPipe.

### Detectors

Using statistics:
- **DescriptiveStatsDetector**: Threshold-based statistics comparison against the baseline.

Using instances:
- **AEDetector** (not implemented yet): Detects changes in the instances using a pre-trained variational autoencoder.
- **MahalanobisDetector** (not implemented yet): Detects changes in the instances comparing the mahalanobis distance with a given threshold.

> *NOTE*: Pre-trained models can be specified as a parameter, by name in case of being trained in hopsworks, or by path to the corresponding storage.

## Drift

*DriftPipe* defines a stage for detecting drift on the inference data given the baseline statistics. There are two types of pipes: *StatsDriftPipe* and *WindowDriftPipe*. The first attempts to detect drift on the inference data based on previously computed statistics (e.g feature distributions), while the second does the same directly from the instances. Therefore, *StatsDriftPipe* can be attached to the StatsPipe while *WindowDriftPipe* is attached directly to the *WindowPipe*.

> *NOTE*: Currently, pipes can be preceded by only one pipe. Support for merging multiple pipes (e.g *WindowPipe*) can enable more efficient drift detection algorithms.

### Detectors

Using statistics:

- **WassersteinDetector**: Computes the Wasserstein distance (or Earth Mover's Distance) between the inference features distributions and the ones provided by the baseline.
- **KullbackLeiblerDetector**: Computes the Kullback-Leibler divergence between the inference features distributions and the ones provided by the baseline.
- **JensenShannonDetector**: Computes the Jensen-Shannon divergence between the inference features distributions and the ones provided by the baseline.

Using features:

- Pending

## Extensibility

The addition of new statistics can be performed by creating a StatDefinition and implementing an aggregator by extending the corresponding *StatSimpleAggregator*, *StatCompoundAggregator* or *StatMultipleAggregator*.

Additional implementations of outlier or drift detectors can be carried out by extending the traits *StatsOutlierDetector* and *WindowOutlierDetector* in case of outliers, or *StatsDriftDetector* and *WindowDriftDetector* in case of drift.

## Examples

### Iris

An example of monitoring a model trained with the Iris dataset can be found in *examples/iris* folder.