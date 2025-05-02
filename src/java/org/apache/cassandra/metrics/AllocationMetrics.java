package org.apache.cassandra.metrics;

import com.yammer.metrics.core.MetricName;

public class AllocationMetrics {
    public final LatencyMetrics latency;

    public AllocationMetrics(String allocatorName)
    {
        this(new AllocationMetricsNameFactory(allocatorName));
    }

    public AllocationMetrics(MetricNameFactory factory)
    {
        this.latency = new LatencyMetrics(factory, "");
    }

    public void release()
    {
        latency.release();
    }

    static class AllocationMetricsNameFactory implements MetricNameFactory
    {
        private final String allocatorName;

        AllocationMetricsNameFactory(String allocatorName)
        {
            this.allocatorName = allocatorName;
        }

        public MetricName createMetricName(String metricName)
        {
            String groupName = AllocationMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=Allocation");
            mbeanName.append(",allocator=").append(allocatorName);
            mbeanName.append(",name=").append(metricName);

            return new MetricName(groupName, "Allocation", metricName, allocatorName, mbeanName.toString());
        }
    }
}
