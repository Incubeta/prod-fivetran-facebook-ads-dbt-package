# Facebook Ads ([Source](https://www.facebook.com/business/ads))

This package models Facebook Ads data from [Fivetran's connector](https://fivetran.com/docs/applications/facebook-ads). It uses data from the Facebook Marketing API in the format described by [this ERD](https://fivetran.com/docs/applications/facebook-ads#schemainformation).

This package enables you to better understand your Facebook Ads performance. Its main focus is to:
- Transform the core social media object tables into analytics-ready models, focusing on ad performance and costs
- Provide clean and organized staging models that can be used as building blocks for your customized reporting
- Enhance the source data with relevant metrics and attributes that are commonly used in social media reporting

## Models

* ad_performance_with_conversion_data_v_1

## Installation Instructions
Check [dbt Hub](https://hub.getdbt.com/) for the latest installation instructions, or [read the dbt docs](https://docs.getdbt.com/docs/package-management) for more information on installing packages.

Include in your `packages.yml`

```yaml
packages:
  - package: https://github.com/Incubeta/prod-fivetran-facebook-ads-dbt-package
    version: v1.0.0
```

## Configuration
By default, this package will look for your Facebook Ads data in the `facebook_ads` schema of your [target database](https://docs.getdbt.com/docs/running-a-dbt-project/using-the-command-line-interface/configure-your-profile). If this is not where your Facebook Ads data is, please add the following configuration to your `dbt_project.yml` file:

```yaml
# dbt_project.yml

...
config-version: 2

vars:
  facebook_ads_source:
    facebook_ads_database: your_database_name
    facebook_ads_schema: your_schema_name 
```

### Changing the Build Schema
By default this package will build the Facebook Ads staging models within a schema titled (<target_schema> + `_stg_facebook_ads`) and the Facebook Ads final models within a schema titled (<target_schema> + `_facebook_ads`) in your target database. If this is not where you would like your modeled Facebook Ads data to be written to, add the following configuration to your `dbt_project.yml` file:

```yaml
# dbt_project.yml

...
models:
  facebook_ads:
    +schema: my_new_schema_name # leave blank for just the target_schema
    staging:
      +schema: my_new_schema_name # leave blank for just the target_schema
```

### Enabling/Disabling Models

This package takes into consideration that not every Facebook Ads account will have the same access to certain fields. If you do not have access to a particular field, you may disable the related functionality by adding the corresponding variable(s) to your `dbt_project.yml` file:

```yaml
vars:
  fivetran_facebook_ads__ad_performance_with_conversion_data_enabled: false      # default is true
```


## Contributions
Don't see a model or specific metric you would have liked to be included? Notice any bugs when installing and running the package? If so, we highly encourage and welcome contributions to this package! 

Please create issues or open PRs against `main`. Check out [this post](https://discourse.getdbt.com/t/contributing-to-a-dbt-package/657) on the best workflow for contributing to a package.

## Database Support
This package has been tested on BigQuery, Snowflake, Redshift, Postgres, and Databricks.

## Resources:
- Provide [feedback](https://www.surveymonkey.com/r/DQ7K7WW) on our existing dbt packages or what you'd like to see next
- Have questions, feedback, or need help? Book a time during our office hours [using Calendly](https://calendly.com/fivetran-solutions-team/fivetran-solutions-team-office-hours) or email us at solutions@fivetran.com
- Find all of Fivetran's pre-built dbt packages in our [dbt hub](https://hub.getdbt.com/fivetran/)
- Learn how to orchestrate [dbt transformations with Fivetran](https://fivetran.com/docs/transformations/dbt)
- Learn more about Fivetran overall [in our docs](https://fivetran.com/docs)
- Check out [Fivetran's blog](https://fivetran.com/blog)
- Learn more about dbt [in the dbt docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the dbt blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
```
