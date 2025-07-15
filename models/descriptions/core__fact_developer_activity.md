{% docs core__fact_developer_activity %}

## Description
This table contains developer activity data sourced from GitHub repositories related to the NEAR Protocol ecosystem, capturing code commits, repository statistics, and development metrics. The data includes information about NEAR-related projects, developer contributions, and ecosystem growth indicators. This table provides insights into the developer community activity, project development velocity, and overall ecosystem health through external development metrics.

## Key Use Cases
- Developer ecosystem analysis and community growth tracking
- Project development velocity and activity monitoring
- Code contribution analysis and developer engagement metrics
- Repository health and maintenance activity tracking
- Ecosystem project discovery and categorization
- Developer retention and activity pattern analysis
- Cross-project collaboration and dependency analysis

## Important Relationships
- Provides external context for blockchain activity analysis
- Supports ecosystem health metrics in `stats.ez_core_metrics_hourly`
- Enables correlation between developer activity and protocol usage
- Provides foundation for developer-focused analytics and reporting
- Supports project categorization and ecosystem mapping

## Commonly-used Fields
- `repo_owner` and `repo_name`: Essential for project identification and categorization
- `endpoint_name`: Important for understanding the type of GitHub data captured
- `data`: Critical for detailed developer activity analysis and metrics
- `provider`: Useful for data source identification and reliability assessment
- `snapshot_timestamp`: Primary field for time-series analysis of developer activity
- `endpoint_github`: Important for linking to specific GitHub API endpoints

{% enddocs %} 