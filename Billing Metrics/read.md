1. Overview
This resource optimization guide represents one module of the four contained in the series. These guides are meant to help customers better monitor and manage their credit consumption. Helping our customers build confidence that their credits are being used efficiently is key to an ongoing successful partnership. In addition to this set of Snowflake Guides for Resource Optimization, Snowflake also offers community support as well as Training and Professional Services offerings. To learn more about the paid offerings, take a look at upcoming education and training.

This blog post can provide you with a better understanding of Snowflake's Resource Optimization capabilities.

Billing Metrics
Billing queries are responsible for identifying total costs associated with the high level functions of the Snowflake Cloud Data Platform, which includes warehouse compute, snowpipe compute, and storage costs. If costs are noticeably higher in one category versus the others, you may want to evaluate what might be causing that.

These metrics also seek to identify those queries that are consuming the most amount of credits. From there, each of these queries can be analyze for their importance (do they need to be run as frequently, if at all) and explore if additional controls need to be in place to prevent excessive consumption (i.e. resource monitors, statement timeouts, etc.).

What You'll Learn
how to identify and analyze Snowflake consumption across all services
how to analyze most resource-intensive queries
how to analyze serverless consumption
What You'll Need
A Snowflake Account
Access to view Account Usage Data Share
Related Materials
Resource Optimization: Setup & Configuration
Resource Optimization: Usage Monitoring
Resource Optimization: Performance