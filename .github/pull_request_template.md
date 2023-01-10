# Description

_Please include a summary of changes and related issue (if any)._


# Tests

- [ ] Please provide evidence of your successful `dbt run` / `dbt test` here
- [ ] Any comparison between `prod` and `dev` for any schema change


# Checklist
- [ ] Follow [dbt style guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)
- [ ] Run `git merge main` to pull any changes from remote into your branch prior to merge.
- [ ] Tag the person(s) responsible for reviewing proposed changes
- [ ] **IMPORTANT** Note for deployment if a `full-refresh` is needed for any model this PR impacts.
     - If any schema is changing, whether it be a table or a view, we will need to run a `full-refresh` in prod to avoid errors with the scheduled refresh jobs.
     - The PR can be reviewed, but should not be merged unless by someone with prod access.
         - Tag `forgxyz` if a full-refresh will be required on prod.
